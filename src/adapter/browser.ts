import type { ILink } from '../bus/link/index.ts'
import { baseLink } from '../bus/link/base.ts'
import { systemClock, type Clock } from '../core/index.ts'
import { report } from '../core/internal.ts'
import type { Unsub } from '../index.ts'

/**
 * Browser transport.
 *
 * Everything is expressed over one target type, {@link Link.Target}: a `Worker`,
 * `self` inside one, a `MessagePort`, a `BroadcastChannel`, or a `Window`
 * wrapped in {@link fromWindow}.
 *
 * {@link link} wraps a target and {@link worker} spawns one, which is the whole
 * API for a page and its workers. {@link connector}, {@link handshake} and
 * {@link bridge} are for peers that connect themselves: each transfers one end
 * of a fresh `MessageChannel` up towards the hub, so a frame's handshake is
 * relayed by its ancestors while its traffic is not — a transferred port
 * survives being passed onward, and ancestors are never in the data path.
 */

/* -------------------------------------------------------------------------- */
/* targets                                                                    */
/* -------------------------------------------------------------------------- */

export declare namespace Link {
  /** Structural, so this works with `window`, `self` in a worker, or a stub. */
  export interface Events {
    addEventListener(type: 'message', fn: (event: MessageEvent) => void): void
    removeEventListener(type: 'message', fn: (event: MessageEvent) => void): void
  }

  /**
   * Anything that posts and receives messages: a `Worker`, `self` inside one, a
   * `MessagePort`, a `BroadcastChannel`, or {@link fromWindow}'s result.
   */
  export interface Target extends Events {
    postMessage(message: unknown, transfer?: Transferable[]): void
    start?(): void
    terminate?(): void
    close?(): void
  }

  export interface Options {
    /** Label for the far end, for logs and policy. Not unique. */
    name?: string | undefined
    /**
     * Terminate or close the target when the link ends. Defaults to true for a
     * target with `terminate` — you only hold a `Worker` handle to one you
     * created — and false otherwise, so `link(self)` never closes the worker it
     * runs in. Set it explicitly either way when the default is wrong.
     */
    own?: boolean | undefined
    /**
     * ms between pings. A `MessagePort` has no disconnect event — a navigated
     * frame or a terminated worker leaves a port that looks perfectly healthy —
     * so this is the only way either side learns the peer is gone. 0 disables.
     */
    heartbeat?: number | undefined
    /** ms to wait for the pong before declaring the peer dead. 0 keeps pinging without a deadline. */
    timeout?: number | undefined
    /** Inbound held before the first `listen`. Losing these loses the greeting. */
    pending?: number | undefined
    clock?: Clock | undefined
    meta?: ILink.Meta | undefined
    /** An uncloneable payload, or a relay that could not hand the port on. */
    onError?: ((err: unknown) => void) | undefined
  }
}

interface WindowLike {
  postMessage(message: unknown, targetOrigin: string, transfer?: Transferable[]): void
}

export interface WindowOptions {
  /** Where inbound events arrive, when that is not the global scope. For tests. */
  on?: Link.Events | undefined
}

/**
 * A `Window` — an iframe, an opener, `parent` — as a {@link Link.Target}.
 *
 * `origin` is required and applied both ways: it is the target origin for every
 * send, and an inbound event must carry it and come from `target`, so a page
 * with several frames does not cross-deliver. Defaulting it to `'*'` would hand
 * a live `MessagePort` to whoever happens to be listening.
 *
 * `target` may be a thunk, so a frame that is not mounted yet still works.
 */
export const fromWindow = (
  target: WindowLike | (() => WindowLike),
  origin: string,
  opts: WindowOptions = {},
): Link.Target => {
  const on = opts.on ?? (globalThis as unknown as Link.Events)
  const at = typeof target === 'function' ? target : () => target
  const wrapped = new Map<(event: MessageEvent) => void, (event: MessageEvent) => void>()
  return {
    postMessage: (message, transfer) => at().postMessage(message, origin, transfer ?? []),
    addEventListener: (_type, fn) => {
      const wrap = (event: MessageEvent) => {
        if (event.origin !== origin || event.source !== (at() as unknown)) return
        fn(event)
      }
      wrapped.set(fn, wrap)
      on.addEventListener('message', wrap)
    },
    removeEventListener: (_type, fn) => {
      const wrap = wrapped.get(fn)
      if (!wrap) return
      wrapped.delete(fn)
      on.removeEventListener('message', wrap)
    },
  }
}

/* -------------------------------------------------------------------------- */
/* link                                                                       */
/* -------------------------------------------------------------------------- */

const dispose = (target: Link.Target) => {
  if (typeof target.terminate === 'function') target.terminate()
  else target.close?.()
}

/**
 * Wrap a target as a link.
 *
 * Both ends answer pings and both may send them, so liveness is symmetric —
 * a one-sided heartbeat only ever tells one of the two sides it is alone.
 */
export const link = <T = any>(port: Link.Target, opts: Link.Options = {}): ILink<T> => {
  const { heartbeat = 5_000, timeout = 2_000, clock = systemClock, meta = {}, name = '' } = opts
  const own = opts.own ?? typeof port.terminate === 'function'

  let seq = 0
  let awaiting = 0
  let cancelBeat: Unsub | null = null
  let cancelPong: Unsub | null = null

  const core = baseLink<T>({
    describe: () => ({ remote: name, meta }),
    pending: opts.pending,
    onError: opts.onError,
    release: () => {
      cancelBeat?.()
      cancelPong?.()
      cancelBeat = cancelPong = null
      port.removeEventListener('message', handle)
      if (own) dispose(port)
    },
  })

  /**
   * A failed clone is a bad payload, not a dead port. Tearing the link down for
   * it would take every other topic on the connection with it, over one
   * caller's mistake.
   */
  const post = (m: unknown): boolean => {
    try {
      port.postMessage(m)
      return true
    } catch (err) {
      if (err instanceof Error && err.name === 'DataCloneError') {
        report(err, opts.onError)
        return false
      }
      core.shut()
      return false
    }
  }

  const beat = () => {
    if (!core.alive) return
    const id = ++seq
    awaiting = id
    if (!post({ kind: CTRL, c: 'ping', id } satisfies Ctrl)) return
    if (timeout > 0) {
      cancelPong = clock.timer(() => {
        if (awaiting === id) core.shut()
      }, timeout)
    } else if (heartbeat > 0) {
      cancelBeat = clock.timer(beat, heartbeat)
    }
  }

  function handle(event: MessageEvent) {
    if (!core.alive) return
    const d: unknown = event.data
    if (isCtrl(d)) {
      switch (d.c) {
        case 'ping':
          post({ kind: CTRL, c: 'pong', id: d.id } satisfies Ctrl)
          return
        case 'pong':
          if (d.id !== awaiting) return
          awaiting = 0
          cancelPong?.()
          cancelPong = null
          if (heartbeat > 0 && timeout > 0) cancelBeat = clock.timer(beat, heartbeat)
          return
        case 'bye':
          // An orderly goodbye, so neither side waits out a heartbeat.
          core.shut()
          return
      }
    }
    core.deliver(d as T)
  }

  port.addEventListener('message', handle)
  port.start?.()

  if (heartbeat > 0) cancelBeat = clock.timer(beat, heartbeat)

  return core.expose(
    (msg) => post(msg),
    () => {
      if (!core.alive) return
      post({ kind: CTRL, c: 'bye' } satisfies Ctrl)
      core.shut()
    },
  )
}

/**
 * Spawn a worker and offer it to a hub: `hub.serve(worker('./a.ts'))`.
 *
 * The worker is created when the hub serves, not when this is called, and is
 * terminated by the returned teardown. Peers reach each other through the hub,
 * so two workers need no channel between them.
 *
 * Messages posted to a worker before its script runs are queued, so the hub's
 * greeting is never lost.
 */
export const worker =
  <T>(url: string | URL, opts: Link.Options & { type?: WorkerType | undefined } = {}): ILink.Source<T> =>
  (onLink) => {
    const one = link<T>(new Worker(url, { type: opts.type ?? 'module' }), { own: true, ...opts })
    onLink(one)
    return () => one.close()
  }

/* -------------------------------------------------------------------------- */
/* handshake                                                                  */
/* -------------------------------------------------------------------------- */

const OPEN = 'wire/open'

type Open = {
  readonly kind: typeof OPEN
  readonly v: 1
  readonly name: string
  /** Origins the handshake was relayed through, nearest frame first. */
  readonly path: readonly string[]
  readonly meta?: ILink.Meta | undefined
}

const isOpen = (d: unknown): d is Open => {
  if (typeof d !== 'object' || d === null) return false
  const o = d as Partial<Open>
  return o.kind === OPEN && o.v === 1 && typeof o.name === 'string' && Array.isArray(o.path)
}

/**
 * Control traffic rides the same port but never reaches `listen`, so consumers
 * above layer 0 never see it. Protocol envelopes carry `$`/`v` and never
 * `kind`, so the two cannot collide.
 */
const CTRL = 'wire/ctrl'

type Ctrl =
  | { readonly kind: typeof CTRL; readonly c: 'ping' | 'pong'; readonly id: number }
  | { readonly kind: typeof CTRL; readonly c: 'bye' }

const isCtrl = (d: unknown): d is Ctrl =>
  typeof d === 'object' && d !== null && (d as Ctrl).kind === CTRL && typeof (d as Ctrl).c === 'string'

/**
 * Mint a channel, send one end up towards the hub, keep the other.
 *
 * Nothing here waits for an answer. If nobody claims the handshake the port
 * simply never carries a greeting, and `Link.persistent` gives up on its own
 * timeout and mints a fresh channel — which is the only reason a fresh
 * `MessageChannel` per attempt matters.
 */
export const connector =
  <T>(name: string, up: Link.Target, opts: Link.Options = {}): ILink.Connector<T> =>
  () => {
    const channel = new MessageChannel()
    up.postMessage({ kind: OPEN, v: 1, name, path: [], meta: opts.meta } satisfies Open, [channel.port2])
    return link<T>(channel.port1, { ...opts, name, own: true })
  }

export declare namespace Handshake {
  export type Origins = readonly string[] | ((origin: string) => boolean)

  /** What a {@link Handshake.Options.filter} is shown about an offered port. */
  export interface Offer {
    readonly name: string
    readonly origin: string
    readonly path: readonly string[]
  }

  export interface Options extends Link.Options {
    /**
     * Defaults to same-origin plus `''` — what a worker sees for messages from
     * its owner, and what some sandboxed frames report. Name any cross-origin
     * frame explicitly.
     */
    origins?: Handshake.Origins | undefined
    /**
     * Selects the handshakes this acceptor or bridge handles, usually by `name`.
     *
     * A `message` event reaches every listener on the target, so declining is
     * never destructive: the port is left untouched for whoever else may want
     * it. Closing it here would kill a peer a second hub on the same window
     * just accepted. The far end is not left hanging either — an unclaimed port
     * never carries a greeting, so `persistent` times out and mints a fresh
     * channel.
     */
    filter?: ((offer: Handshake.Offer) => boolean) | undefined
    signal?: AbortSignal | undefined
  }
}

const allower = (origins?: Handshake.Origins) => {
  if (typeof origins === 'function') return origins
  const own = typeof location === 'undefined' ? '' : location.origin
  const set = new Set<string>(origins ?? ['', own])
  return (origin: string) => set.has(origin)
}

const bind = (target: Link.Events, handle: (e: MessageEvent) => void, signal?: AbortSignal): Unsub => {
  target.addEventListener('message', handle)
  let done = false
  const stop: Unsub = () => {
    if (done) return
    done = true
    signal?.removeEventListener('abort', stop)
    target.removeEventListener('message', handle)
  }
  if (signal?.aborted) stop()
  else signal?.addEventListener('abort', stop, { once: true })
  return stop
}

/** Claim a handshake, or decline it without touching the port. */
const claim = (
  opts: Handshake.Options,
  allowed: (origin: string) => boolean,
  event: MessageEvent,
): { open: Open; port: MessagePort } | null => {
  const d: unknown = event.data
  if (!isOpen(d)) return null
  const port = event.ports[0]
  if (!port) return null
  if (!allowed(event.origin)) return null
  let wanted = true
  try {
    wanted = opts.filter?.({ name: d.name, origin: event.origin, path: d.path }) !== false
  } catch (err) {
    report(err, opts.onError)
    return null
  }
  return wanted ? { open: d, port } : null
}

/**
 * Accept transferred ports as peers: `hub.serve(handshake(self))` in a worker
 * serving a page and its frames.
 *
 * Several hubs can share one target — that is what the envelope's `$` tag is
 * for — but a transferred port must reach exactly one handler. Two of them
 * wrapping the same port means the first to close disconnects the other, and a
 * `bridge` that re-transfers a port another handler has already started will
 * throw. Partition with `filter`.
 */
export const handshake = <T>(target: Link.Events, opts: Handshake.Options = {}): ILink.Source<T> => {
  const allowed = allower(opts.origins)
  return (onLink) =>
    bind(
      target,
      (event) => {
        const got = claim(opts, allowed, event)
        if (!got) return

        onLink(
          link<T>(got.port, {
            ...opts,
            name: got.open.name,
            own: true,
            meta: { ...got.open.meta, origin: event.origin, path: [event.origin, ...got.open.path] },
          }),
        )
      },
      opts.signal,
    )
}

/**
 * Relay a descendant frame's handshake one hop closer to the hub. Only one
 * handler per target may claim a given port — see {@link handshake} — so a page
 * that both hosts a hub and relays for its frames must partition them with
 * `filter`.
 *
 * Each hop records the origin it came from, so the hub's `peer.meta.path` names
 * the whole chain and a policy can act on it. Spreading `event.data` through
 * unchecked would forward any object with the right shape, from any origin,
 * straight to the worker.
 */
export const bridge = (target: Link.Events, up: Link.Target, opts: Handshake.Options = {}): Unsub => {
  const allowed = allower(opts.origins)
  return bind(
    target,
    (event) => {
      const got = claim(opts, allowed, event)
      if (!got) return
      const next: Open = { ...got.open, path: [event.origin, ...got.open.path] }
      try {
        up.postMessage(next, [got.port])
      } catch (err) {
        // Throws if another handler on this target already started the port —
        // the exact collision `filter` exists to prevent. Reported rather than
        // thrown out of a `message` handler, where nothing can catch it.
        report(err, opts.onError)
      }
    },
    opts.signal,
  )
}
