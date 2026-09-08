import { detacher, systemClock, type Clock, type Unsub } from '../../core/index.ts'
import { attempt, report } from '../../core/internal.ts'
import { defineLink } from './base.ts'
import { outbox } from './outbox.ts'

import type { Link } from './index.ts'

export interface Persistent<T> extends Link<T> {
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
export const persistent = <T>(connector: Link.Connector<T>, opts: Persistent.Options = {}): Persistent<T> => {
  const { base = 250, max = 30_000 } = opts.backoff ?? {}
  const clock = opts.clock ?? systemClock
  const jitter = Math.min(Math.max(opts.jitter ?? 0, 0), 1)
  const timeout = opts.timeout ?? 10_000
  const limit = opts.maxAttempts ?? 0

  const held = outbox<T>({ ...opts, clock, onDrop: opts.onDrop })
  let inner: Link<T> | null = null
  let state: Persistent.State = 'connecting'
  let remote = ''
  let meta: Link.Meta = {}
  let fails = 0
  let epoch = 0
  let cancel: Unsub | null = null
  let detach: Unsub | null = null

  // Assigned by `defineLink` before anything below can run: `connect` is
  // driven from inside `open`, which is the only caller that reaches it first.
  let host!: Link.Host<T>

  const setState = (next: Persistent.State) => {
    if (next === state) return
    const was = state === 'open'
    state = next
    attempt(() => opts.onState?.(next), undefined, opts.onError)
    const is = next === 'open'
    if (is !== was) host.signal(is)
  }

  const flush = () => {
    const live = inner
    if (live) held.drain((msg) => live.send(msg))
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
    let link: Link<T>
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

    const stops = detacher()
    const stop = stops.stop
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
    stops.add(
      link.listen((m) => {
        if (token !== epoch) return
        if (state !== 'open') {
          cancelExpiry?.()
          cancelExpiry = null
          fails = 0
          setState('open')
          flush()
        }
        host.deliver(m)
      }),
    )
    // `closed` fires synchronously here if the connector handed back a dead
    // link. The detacher runs each teardown as it arrives once `stop` has
    // already gone, and `dropped` is guarded by the attempt token.
    stops.add(link.closed(dropped))

    if (timeout > 0) {
      cancelExpiry = clock.timer(expired, timeout)
      stops.add(() => cancelExpiry?.())
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
    held.clear()
    setState('closed')
    host.shut()
  }

  const link = defineLink<T>(
    (h) => {
      host = h
      // The first attempt runs here rather than before the link exists, so a
      // connector that hands back a dead link — or throws — has somewhere to
      // report it.
      connect()
      return {
        send: (msg) => {
          if (state === 'closed') return false
          if (state === 'open' && inner) return inner.send(msg)
          return held.hold(msg)
        },
        close,
      }
    },
    {
      // Thunks: the far end changes under this link on every reconnect.
      remote: () => remote,
      meta: () => meta,
      pending: opts.pending,
      // A link that has not connected yet is alive and down, so the first
      // `open` reads as a transition rather than a repeat `changed` swallows.
      up: false,
      onError: opts.onError,
      onDrop: (m) => opts.onDrop?.(m),
    },
  )

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
