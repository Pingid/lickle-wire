import { type Unsub, type Clock, systemClock } from '../../core/index.ts'
import { attempt, report } from '../../core/internal.ts'
import { baseLink } from './base.ts'

import type { ILink } from './index.ts'

export interface Persistent<T> extends ILink<T> {
  readonly state: Persistent.State
  /** Consecutive failures since the last success. */
  readonly attempts: number
  /**
   * Abandon any pending wait and reconnect now, resetting backoff. For
   * `online` and `visibilitychange` handlers, where the host knows something
   * the backoff schedule does not.
   */
  retryNow(): void
}

export declare namespace Persistent {
  export type State = 'connecting' | 'open' | 'retrying' | 'closed'

  export interface Options {
    /** Bounds for links that die before ever delivering a frame. */
    backoff?: { base?: number; max?: number } | undefined
    /**
     * 0..1 randomisation of each wait, applied symmetrically around it. A
     * one-sided version only ever shortens the wait, which drags the effective
     * base toward zero and turns a fast-failing connector into a hot loop.
     */
    jitter?: number | undefined
    /**
     * How long a fresh link has to deliver its first message before the attempt
     * is abandoned. Guards the case a dead port cannot report: chrome hands back
     * an open-looking port when nothing is listening. 0 disables.
     */
    timeout?: number | undefined
    /** Consecutive failures before giving up and closing for good. 0 is forever. */
    maxAttempts?: number | undefined
    /**
     * Outbound messages held while the link is down. Overflow is refused at the
     * call site: `send` returns false and `onDrop` fires. Set 0 to never buffer.
     */
    buffer?: number | undefined
    /**
     * ms a buffered message may wait before it is dropped. Without this, a send
     * during a long outage is delivered minutes later with no way for the caller
     * to have known. 0 is forever.
     */
    ttl?: number | undefined
    /** Which end of a full buffer loses. Defaults to refusing the new message. */
    overflow?: 'oldest' | 'newest' | undefined
    /** Inbound held before the first `listen`. Covers a late-attached session. */
    pending?: number | undefined
    clock?: Clock | undefined
    onState?: ((state: State) => void) | undefined
    /** A throwing connector, an expired attempt, or exhausted attempts. */
    onError?: ((err: unknown) => void) | undefined
    /** A message that will never be delivered. */
    onDrop?: ((msg: unknown) => void) | undefined
  }
}

/**
 * A link that outlives its transport. `up` is false while it is down, `closed`
 * fires only when you close it or give up, and reconnection is scheduled rather
 * than deferred to the next send — a lazy design leaves a subscribe-only client
 * disconnected forever, because nothing it does triggers a reconnect.
 *
 * A connection counts as successful once it delivers its first inbound message.
 * That is why layer 1 has a `ready` frame: without an unprompted greeting from
 * the other end, "we connected but nobody was there" is indistinguishable from
 * "we connected and it has been quiet", and every attempt looks like a failure.
 */
/** Not public: one outbound message waiting for the link to come up. */
type Held<T> = { msg: T; at: number }

export const persistent = <T>(connector: ILink.Connector<T>, opts: Persistent.Options = {}): Persistent<T> => {
  const { base = 250, max = 30_000 } = opts.backoff ?? {}
  const clock = opts.clock ?? systemClock
  const cap = opts.buffer ?? 64
  const ttl = opts.ttl ?? 0
  const overflow = opts.overflow ?? 'newest'
  const jitter = Math.min(Math.max(opts.jitter ?? 0, 0), 1)
  const timeout = opts.timeout ?? 10_000
  const limit = opts.maxAttempts ?? 0

  const outbox: Held<T>[] = []
  let inner: ILink<T> | null = null
  let state: Persistent.State = 'connecting'
  let remote = ''
  let meta: ILink.Meta = {}
  let fails = 0
  let epoch = 0
  let cancel: Unsub | null = null
  let detach: Unsub | null = null

  const core = baseLink<T>({
    describe: () => ({ remote, meta }),
    pending: opts.pending,
    up: () => state === 'open',
    onError: opts.onError,
    onDrop: (m) => opts.onDrop?.(m),
  })

  const setState = (next: Persistent.State) => {
    if (next === state) return
    const was = state === 'open'
    state = next
    attempt(() => opts.onState?.(next), undefined, opts.onError)
    const is = next === 'open'
    if (is !== was) core.signal(is)
  }

  const expire = () => {
    if (ttl <= 0) return
    const cutoff = clock.now() - ttl
    while (outbox.length > 0 && (outbox[0] as Held<T>).at < cutoff) {
      const stale = outbox.shift() as Held<T>
      opts.onDrop?.(stale.msg)
    }
  }

  const flush = () => {
    if (!inner) return
    expire()
    for (const held of outbox.splice(0)) if (!inner.send(held.msg)) opts.onDrop?.(held.msg)
  }

  const giveUp = (err: unknown) => {
    report(err, opts.onError)
    close()
  }

  const schedule = () => {
    if (state === 'closed') return
    fails += 1
    if (limit > 0 && fails >= limit) {
      giveUp(new Error(`giving up on ${remote || 'peer'} after ${fails} attempts`))
      return
    }
    setState('retrying')
    const wait = Math.min(base * 2 ** (fails - 1), max)
    const spread = wait * jitter
    const delay = Math.max(0, wait - spread + Math.random() * spread * 2)
    cancel = clock.timer(connect, delay)
  }

  function connect() {
    if (state === 'closed') return
    cancel?.()
    cancel = null

    const token = ++epoch
    let link: ILink<T>
    try {
      link = connector()
    } catch (err) {
      report(err, opts.onError)
      schedule()
      return
    }

    inner = link
    remote = link.remote
    meta = link.meta
    setState('connecting')

    const stops: Unsub[] = []
    const stop = () => {
      for (const s of stops) s()
      stops.length = 0
    }
    const dropped = () => {
      if (token !== epoch) return
      epoch += 1
      stop()
      inner = null
      schedule()
    }
    const expired = () => {
      if (token !== epoch || state === 'open') return
      epoch += 1
      stop()
      const dead = inner
      inner = null
      dead?.close()
      report(new Error(`no greeting from ${remote || 'peer'} within ${timeout}ms`), opts.onError)
      schedule()
    }

    let cancelExpiry: Unsub | null = null
    stops.push(
      link.listen((m) => {
        if (token !== epoch) return
        if (state !== 'open') {
          cancelExpiry?.()
          cancelExpiry = null
          fails = 0
          setState('open')
          flush()
        }
        core.deliver(m)
      }),
    )
    // `closed` may fire synchronously here if the connector handed back a dead
    // link, which is why `dropped` is guarded by the attempt token.
    stops.push(link.closed(dropped))

    if (timeout > 0) {
      cancelExpiry = clock.timer(expired, timeout)
      stops.push(() => cancelExpiry?.())
    }

    if (token !== epoch) {
      stop()
      return
    }
    detach = stop
  }

  function close() {
    if (state === 'closed') return
    epoch += 1
    cancel?.()
    cancel = null
    detach?.()
    detach = null
    inner?.close()
    inner = null
    for (const held of outbox.splice(0)) opts.onDrop?.(held.msg)
    setState('closed')
    core.shut()
  }

  connect()

  const link = core.expose((msg) => {
    if (state === 'closed') return false
    if (state === 'open' && inner) return inner.send(msg)
    if (cap <= 0) {
      opts.onDrop?.(msg)
      return false
    }
    expire()
    if (outbox.length >= cap) {
      if (overflow === 'newest') {
        opts.onDrop?.(msg)
        return false
      }
      while (outbox.length >= cap) {
        const oldest = outbox.shift() as Held<T>
        opts.onDrop?.(oldest.msg)
      }
    }
    outbox.push({ msg, at: clock.now() })
    return true
  }, close)

  return {
    ...link,
    get remote() {
      return link.remote
    },
    get meta() {
      return link.meta
    },
    get up() {
      return link.up
    },
    get state() {
      return state
    },
    get attempts() {
      return fails
    },
    retryNow() {
      if (state === 'closed' || state === 'open') return
      cancel?.()
      cancel = null
      detach?.()
      detach = null
      inner?.close()
      inner = null
      fails = 0
      connect()
    },
  }
}
