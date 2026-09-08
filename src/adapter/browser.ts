import { systemClock, type Clock, type ListenOptions, type Port, type Unsub } from '../core/index.ts'
import { attempt, noop, report } from '../core/internal.ts'
import {
  accept,
  defineLink,
  defineSource,
  passOffer,
  pipe,
  postOffer,
  readOffers,
  relay,
  type Accept,
  type Link,
  type Mux,
  type Transfer,
  type TransferPort,
} from '../bus/link/index.ts'
import { keepalive } from './keepalive.ts'
import type { Envelope } from '../bus/protocol.ts'

/**
 * Browser transport.
 *
 * Thin by construction. The DOM has no single "thing you post messages to", so
 * what this module really contributes is the type layer — {@link Browser.Target}
 * is a union of the two shapes the platform actually has — plus two
 * conversions, {@link asPort} and {@link asTarget}, that move between the DOM's
 * event/postMessage world and wire's {@link Port}.
 *
 * Everything else is composition over generics: {@link link} is `defineLink`
 * plus {@link keepalive}, {@link handshake} is `accept`, {@link bridge} is
 * `relay`. Nothing here re-implements policy or lifecycle.
 */
export declare namespace Browser {
  /**
   * The receive half, DOM shape: `self`, a `Worker`, a `MessagePort`,
   * `navigator.serviceWorker`, a stub.
   *
   * `MessageEvent`, not `Event`: every DOM `addEventListener` is overloaded and
   * satisfies the narrower listener happily, while `Event` costs a cast at
   * every use and rejects a `(e: MessageEvent) => void` handler at the boundary.
   */
  export interface MessageEventTarget {
    addEventListener(type: 'message', fn: (event: MessageEvent) => void): void
    removeEventListener(type: 'message', fn: (event: MessageEvent) => void): void
  }

  /**
   * The receive half, function shape. Returns its own teardown, which is what
   * lets a filtering or decoding wrapper exist without keeping a map from the
   * caller's handler back to the one actually registered.
   */
  export type Listen = (fn: (event: MessageEvent) => void) => Unsub

  export type Inbound = MessageEventTarget | Listen

  /** The send half. `transfer` is optional, so a `BroadcastChannel` fits. */
  export interface Post {
    postMessage(message: unknown, transfer?: Transferable[]): void
  }

  /** Honoured when present. Nothing here ever invents one. */
  export interface Lifecycle {
    start?(): void
    terminate?(): void
    close?(): void
  }

  /** The send half plus whatever lifecycle it came with. */
  export type Sink = Post & Lifecycle

  /** One object doing both: a `Worker`, a `MessagePort`, a `BroadcastChannel`. */
  export interface Duplex extends MessageEventTarget, Post, Lifecycle {}

  /**
   * Send and receive on different objects: you post to a `ServiceWorker` and
   * hear on `navigator.serviceWorker`; you post to a `Window` and hear on your
   * own. `Split` is the only shape without `postMessage`, which is what the
   * normalisers key off.
   */
  export interface Split<F extends Inbound = Inbound> {
    readonly to: Sink
    readonly from: F
  }

  export type Target = Duplex | Split

  /** Anywhere a message only has to go out: {@link connector}'s and {@link bridge}'s `up`. */
  export type Outbound = Sink | Split

  /**
   * Either the DOM shape or something that already speaks offers. Everything
   * here takes the union, so a page passes a `Window` and a platform that is
   * not the DOM passes its own {@link TransferPort}.
   */
  export type Offering = Outbound | Transfer.Offers<MessagePort>
  export type Accepting = Inbound | Transfer.Offers<MessagePort>

  export interface Options {
    /** Label for the far end, for logs and policy. Not unique. */
    name?: string | undefined
    /**
     * Terminate or close the target when the link ends. Defaults to true for a
     * target with `terminate` — you only hold a `Worker` handle to one you
     * created — and false otherwise, so `link(workerSelf())` never closes the
     * worker it runs in. Set it explicitly either way when the default is wrong.
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
    meta?: Link.Meta | undefined
    /** An uncloneable payload, or a relay that could not hand the port on. */
    onError?: ((err: unknown) => void) | undefined
  }

  /** {@link asPort} has no lifecycle to configure, so this is only error routing. */
  export interface PortOptions {
    /** An uncloneable payload, or a throwing listener. */
    onError?: ((err: unknown) => void) | undefined
  }
}

/* -------------------------------------------------------------------------- */
/* normalisers                                                                */
/* -------------------------------------------------------------------------- */

/** A `Split` is the only target shape without `postMessage` of its own. */
const sink = (up: Browser.Outbound): Browser.Sink => ('postMessage' in up ? up : up.to)

const halves = (target: Browser.Target): { to: Browser.Sink; from: Browser.Inbound } =>
  'postMessage' in target ? { to: target, from: target } : target

/** Both inbound shapes collapse to the function one; nothing below sees the other. */
const listener = (from: Browser.Inbound): Browser.Listen =>
  typeof from === 'function'
    ? from
    : (fn) => {
        from.addEventListener('message', fn)
        return () => from.removeEventListener('message', fn)
      }

/**
 * `postMessage`, made total.
 *
 * A failed clone is a bad payload, not a dead port: tearing the connection down
 * for it would take every other topic on it with it, over one caller's mistake.
 * Anything else means the far end is gone, and only the caller knows whether
 * there is a link to end — hence `fatal`.
 */
const poster =
  (to: Browser.Sink, onError: ((err: unknown) => void) | undefined, fatal: (err: unknown) => void) =>
  (message: unknown, transfer?: readonly Transferable[]): boolean => {
    try {
      // Only ever two arguments when there is something to hand over: a
      // `Window` sink reads its second parameter as a target origin.
      if (transfer && transfer.length > 0) to.postMessage(message, [...transfer])
      else to.postMessage(message)
      return true
    } catch (err) {
      if (err instanceof Error && err.name === 'DataCloneError') {
        report(err, onError)
        return false
      }
      fatal(err)
      return false
    }
  }

const dispose = (to: Browser.Sink) => {
  if (typeof to.terminate === 'function') to.terminate()
  else to.close?.()
}

/* -------------------------------------------------------------------------- */
/* conversions                                                               */
/* -------------------------------------------------------------------------- */

/**
 * The listener bookkeeping the `Port` contract asks for — attach on the first,
 * detach on the last, iterate a copy — shared by both conversions, which differ
 * only in the frame they hand over.
 */
const inbound = <W>(
  from: Browser.Inbound,
  begin: (() => void) | undefined,
  frame: (event: MessageEvent) => W,
  onError: ((err: unknown) => void) | undefined,
) => {
  const on = listener(from)
  const fns = new Set<(value: W) => void>()
  let off: Unsub | null = null

  return (next: (value: W) => void, o: ListenOptions = {}): Unsub => {
    if (o.signal?.aborted) return noop
    fns.add(next)
    if (fns.size === 1) {
      off = on((event) => {
        const value = frame(event)
        // A copy: a listener may detach itself, or another, from inside its
        // own callback, and the `Port` contract requires that to be safe.
        for (const fn of [...fns]) attempt(() => fn(value), undefined, onError)
      })
      begin?.()
    }
    let done = false
    const detach = () => {
      if (done) return
      done = true
      fns.delete(next)
      o.signal?.removeEventListener('abort', detach)
      if (fns.size > 0) return
      off?.()
      off = null
    }
    o.signal?.addEventListener('abort', detach, { once: true })
    return detach
  }
}

/**
 * A target as a bare {@link Port} — two members, no lifecycle, no heartbeat, no
 * options to get wrong.
 *
 * This is the whole of what the DOM contributes, and it is the on-ramp for
 * everything that only ever needed two members: `rpc.left(asPort(worker))`,
 * `replica.reader(def, asPort(channel))`. When liveness is wanted, `asLink`
 * lifts it, or {@link link} does both at once.
 *
 * Attaches on the first listener and detaches on the last, like every `Port`.
 * `onClose` never fires: a `MessagePort` has no disconnect event, which is
 * exactly what {@link link}'s heartbeat exists to cover.
 */
export const asPort = <T>(target: Browser.Target, opts: Browser.PortOptions = {}): Port<T, T> => {
  const { to, from } = halves(target)
  const post = poster(to, opts.onError, (err) => report(err, opts.onError))
  return {
    send: (value) => void post(value),
    listen: inbound<T>(
      from,
      () => to.start?.(),
      (event) => event.data as T,
      opts.onError,
    ),
  }
}

/**
 * A target as a {@link TransferPort}: the same two members, over frames that
 * carry channels rather than only values.
 *
 * This is what lets the handshake be generic. `postMessage(data, transfer)` and
 * `event.ports`/`event.origin` are the only DOM-shaped parts of moving a
 * connection, and they stop here — everything above works the same over an
 * extension runtime or a server.
 */
export const asTransferPort = <S, R = S>(
  target: Browser.Target,
  opts: Browser.PortOptions = {},
): TransferPort<S, R, MessagePort> => {
  const { to, from } = halves(target)
  return { transfers: true, send: sends<S>(to, opts), listen: receives<R>(to, from, opts) }
}

const sends =
  <S>(to: Browser.Sink, opts: Browser.PortOptions) =>
  (msg: Transfer.Out<S, MessagePort>): void => {
    void poster(to, opts.onError, (err) => report(err, opts.onError))(msg.data, msg.transfer)
  }

const receives = <R>(to: Browser.Sink | undefined, from: Browser.Inbound, opts: Browser.PortOptions) =>
  inbound<Transfer.In<R, MessagePort>>(
    from,
    () => to?.start?.(),
    (event) => ({ data: event.data as R, transfer: event.ports, origin: event.origin }),
    opts.onError,
  )

/**
 * A DOM half, or something that already speaks offers, as one.
 *
 * The unused half of each is not silently inert: an acceptor that tries to send
 * or a relay target that is read has been wired up wrongly, and says so.
 */
const accepting = (from: Browser.Accepting, opts: Browser.PortOptions): Transfer.Offers<MessagePort> => {
  if (typeof from !== 'function' && 'listen' in from) return from
  return {
    send: () => report(new Error('this end only reads offers; it has nothing to send on'), opts.onError),
    listen: receives<Transfer.Open>(undefined, from as Browser.Inbound, opts),
  }
}

const offering = (up: Browser.Offering, opts: Browser.PortOptions): Transfer.Offers<MessagePort> => {
  if ('send' in up) return up
  return {
    send: sends<Transfer.Open>(sink(up), opts),
    listen: () => noop,
  }
}

/**
 * The other direction: any {@link Port} as a target, so a hub topic, an rpc
 * side or a link can be handed to code that speaks `postMessage` — and so
 * adapters stack, since the result is a {@link Browser.Duplex} that
 * {@link asPort} and {@link link} accept in turn.
 *
 * The listener map is inherent here rather than incidental: `removeEventListener`
 * identifies a handler by reference, so the wrapper actually registered has to
 * be findable from it.
 *
 * `close` detaches, and forwards to the source's own `close` when it has one —
 * which is what makes a `Link` usable as a target without losing its lifecycle.
 */
export const asTarget = <T>(port: Port<T, T> & { close?(): void }, opts: Browser.PortOptions = {}): Browser.Duplex => {
  const offs = new Map<(event: MessageEvent) => void, Unsub>()
  const detachAll = () => {
    for (const off of offs.values()) off()
    offs.clear()
  }
  return {
    postMessage: (message, transfer) => {
      // A `Port` moves values, not ownership. Dropping a transfer list here
      // would leave a handshake half-sent: the `Open` frame arrives, the port
      // never does, and the far end retries forever with nothing to see.
      if (transfer && transfer.length > 0) {
        if ((port as TransferPort<T, T, unknown>).transfers)
          (port as TransferPort<T, T, unknown>).send({
            data: message as T,
            transfer: transfer as readonly Transferable[],
          })
        report(new Error('asTarget: a Port has no transfer list; this message was not sent'), opts.onError)
        return
      }
      port.send(message as T)
    },
    addEventListener: (_type, fn) => {
      if (offs.has(fn)) return
      offs.set(
        fn,
        port.listen((message) => fn(new MessageEvent('message', { data: message }))),
      )
    },
    removeEventListener: (_type, fn) => {
      offs.get(fn)?.()
      offs.delete(fn)
    },
    close: () => {
      detachAll()
      port.close?.()
    },
  }
}

/**
 * A link that is also a target: one object with both faces, rather than a link
 * and a separate view of it.
 *
 * {@link asTarget} converts and forgets — what comes back is a `Browser.Duplex`
 * and nothing else, so a caller who still needs `up`, `changed` or `closed`
 * ends up holding two handles to one connection and has to keep them straight.
 * This keeps both, which is what you want when the thing being handed over is
 * not a raw port: a mux channel, a slot, a hub topic given to a library that
 * speaks `postMessage`.
 *
 * `close` is the link's, so whichever face a caller reaches for ends the same
 * connection once. There is no `start` or `terminate`: a link is already
 * running and is not a resource this can destroy.
 */
export const asDuplex = <T>(inner: Link<T>, opts: Browser.PortOptions = {}): Link<T> & Browser.Duplex => {
  const face = asTarget<T>(inner, opts)
  return {
    postMessage: face.postMessage,
    addEventListener: face.addEventListener,
    removeEventListener: face.removeEventListener,
    send: (msg) => inner.send(msg),
    listen: (fn, o) => inner.listen(fn, o),
    changed: (fn, o) => inner.changed(fn, o),
    closed: (fn, o) => inner.closed(fn, o),
    // Getters, not a spread: `remote` and `meta` move under a `persistent` or
    // a `slot`, and a snapshot would freeze them at the moment of conversion.
    get remote() {
      return inner.remote
    },
    get meta() {
      return inner.meta
    },
    get up() {
      return inner.up
    },
    // Detaches the synthesised listeners, then ends the link.
    close: () => void face.close?.(),
  }
}

/* -------------------------------------------------------------------------- */
/* targets that are not one object                                            */
/* -------------------------------------------------------------------------- */

interface WindowLike {
  postMessage(message: unknown, targetOrigin: string, transfer?: Transferable[]): void
}

export interface WindowOptions {
  /** Where inbound events arrive, when that is not the global scope. For tests. */
  on?: Browser.Inbound | undefined
}

/**
 * A `Window` — an iframe, an opener, `parent` — as a target.
 *
 * A `Window` is the archetypal {@link Browser.Split}: its `postMessage` demands
 * a target origin, and the events it sends you arrive on your own global, not
 * on it.
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
): Browser.Split<Browser.Listen> => {
  const at = typeof target === 'function' ? target : () => target
  const on = listener(opts.on ?? (globalThis as unknown as Browser.MessageEventTarget))
  return {
    to: { postMessage: (message, transfer) => at().postMessage(message, origin, transfer ?? []) },
    from: (fn) =>
      on((event) => {
        if (event.origin !== origin || event.source !== (at() as unknown)) return
        fn(event)
      }),
  }
}

/**
 * `self` inside a worker as a target.
 *
 * A cast, not an oversight: under `lib: ["DOM"]` — which a page and its workers
 * usually share — `globalThis` is typed as a `Window`, whose `postMessage`
 * demands a `targetOrigin`, and `DedicatedWorkerGlobalScope` does not exist
 * there at all. There is nothing honest left to name.
 */
export const workerSelf = (): Browser.Duplex => globalThis as unknown as Browser.Duplex

/**
 * The active service worker as a target: you post to a `ServiceWorker` and hear
 * on `navigator.serviceWorker`. A thunk, because `controller` changes on update
 * and again on claim.
 */
export const fromServiceWorker = (
  to: ServiceWorker | (() => ServiceWorker | null),
  from: Browser.Inbound = navigator.serviceWorker,
): Browser.Split => {
  const at = typeof to === 'function' ? to : () => to
  return { to: { postMessage: (message, transfer) => at()?.postMessage(message, transfer ?? []) }, from }
}

/* -------------------------------------------------------------------------- */
/* link                                                                       */
/* -------------------------------------------------------------------------- */

/**
 * Wrap a target as a link: {@link asPort}'s normalisation, plus liveness,
 * plus ownership.
 *
 * Both ends answer pings and both may send them, so liveness is symmetric — a
 * one-sided heartbeat only ever tells one of the two sides it is alone.
 */
export const link = <T = any>(target: Browser.Target, opts: Browser.Options = {}): Link<T> => {
  const { heartbeat = 5_000, timeout = 2_000, clock = systemClock, meta = {}, name = '' } = opts
  const { to, from } = halves(target)
  const own = opts.own ?? typeof to.terminate === 'function'

  return defineLink<T>(
    (host) => {
      const post = poster(to, opts.onError, () => host.shut())
      const beat = keepalive(post, host.shut, { interval: heartbeat, timeout, clock })
      const off = listener(from)((event) => {
        const d: unknown = event.data
        if (beat.inbound(d)) return
        host.deliver(d as T)
      })
      to.start?.()
      beat.start()

      return {
        send: post,
        // An orderly goodbye, so neither side waits out a heartbeat.
        close: beat.farewell,
        release: () => {
          beat.stop()
          off()
          if (own) dispose(to)
        },
      }
    },
    { remote: name, meta, pending: opts.pending, onError: opts.onError },
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
 *
 * `defineSource`, not `accept`: nothing is being admitted here. A spawned
 * worker has no offer, no origin and no relay path, and running it through the
 * acceptor would stamp empty ones into its `meta`.
 */
export const worker = <T>(
  url: string | URL,
  opts: Browser.Options & { type?: WorkerType | undefined } = {},
): Link.Source<T> =>
  defineSource<T>((host) => {
    host.offer(link<T>(new Worker(url, { type: opts.type ?? 'module' }), { own: true, ...opts }))
  }, opts)

/* -------------------------------------------------------------------------- */
/* handshake                                                                  */
/* -------------------------------------------------------------------------- */

/**
 * Same-origin plus `''` — what a worker sees for messages from its owner, and
 * what some sandboxed frames report. Any cross-origin frame is named explicitly.
 */
const sameOrigin = (): readonly string[] => ['', typeof location === 'undefined' ? '' : location.origin]

export declare namespace Handshake {
  export type Origins = Accept.Origins

  /** What a {@link Handshake.Options.filter} is shown about an offered port. */
  export type Offer = Accept.Details

  export interface Options extends Browser.Options {
    /** Defaults to same-origin plus `''`. Name any cross-origin frame explicitly. */
    origins?: Origins | undefined
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
    filter?: ((offer: Offer) => boolean) | undefined
    signal?: AbortSignal | undefined
    /** Per-connection facts only this end knows. Merged over what the peer claimed. */
    extract?: ((offer: Accept.Offer<MessagePort>) => Link.Meta | undefined) | undefined
  }
}

/**
 * Mint a channel, send one end up towards the hub, keep the other.
 *
 * Nothing here waits for an answer. If nobody claims the handshake the port
 * simply never carries a greeting, and `persistent` gives up on its own timeout
 * and mints a fresh channel — which is the only reason a fresh `MessageChannel`
 * per attempt matters.
 *
 * `up` is a {@link TransferPort}, which is what says it can carry a channel at
 * all — a plain `Port` cannot, and now will not typecheck here.
 */
export const connector =
  <T>(name: string, up: Browser.Offering, opts: Browser.Options = {}): Link.Connector<T> =>
  () => {
    const channel = new MessageChannel()
    postOffer(offering(up, opts), name, channel.port2, opts.meta)
    return link<T>(channel.port1, { ...opts, name, own: true })
  }

/**
 * Accept transferred ports as peers: `hub.serve(handshake(workerSelf()))` in a
 * worker serving a page and its frames.
 *
 * Several hubs can share one target — that is what the envelope's `$` tag is
 * for — but a transferred port must reach exactly one handler. Two of them
 * wrapping the same port means the first to close disconnects the other, and a
 * `bridge` that re-transfers a port another handler has already started will
 * throw. Partition with `filter`.
 */
export const handshake = <T extends Envelope>(from: Browser.Accepting, opts: Handshake.Options = {}): Link.Source<T> =>
  accept<T, MessagePort>(
    readOffers(accepting(from, opts)),
    (offer, meta) => link<T>(offer.channel, { ...opts, name: offer.name, own: true, meta }),
    { ...opts, origins: opts.origins ?? sameOrigin() },
  )

/**
 * Relay a descendant frame's handshake one hop closer to the hub. Only one
 * handler per target may claim a given port — see {@link handshake} — so a page
 * that both hosts a hub and relays for its frames must partition them with
 * `filter`.
 *
 * Re-transferring the port is what keeps ancestors out of the data path: a
 * transferred port survives being passed onward. Rebuilding the frame rather
 * than spreading `event.data` through is deliberate — spreading would forward
 * any object with the right shape, from any origin, straight to the worker.
 */
export const bridge = (from: Browser.Accepting, up: Browser.Offering, opts: Handshake.Options = {}): Unsub =>
  relay<MessagePort>(readOffers(accepting(from, opts)), passOffer(offering(up, opts)), {
    ...opts,
    origins: opts.origins ?? sameOrigin(),
  })

/**
 * Relay a descendant frame's handshake into a channel on a link you already
 * hold, for when the next hop cannot take a transferred port — the hub is on a
 * server, in an extension, or simply behind a `Link` rather than a `Window`.
 *
 * The difference from {@link bridge} is the whole reason both exist, and it is
 * in the name: a bridge hands the port on and steps out of the way, a tunnel
 * carries the traffic. Reach for `bridge` whenever the next hop can take a
 * port, and for this when it cannot.
 */
export const tunnel = <T>(from: Browser.Accepting, up: Mux<T>, opts: Handshake.Options = {}): Unsub =>
  relay<MessagePort>(
    readOffers(accepting(from, opts)),
    (offer) => {
      // `relay` has already vetted the offer and recorded this hop.
      const near = link<T>(offer.channel, { ...opts, name: offer.name, own: true })
      const far = up.open(offer.name, {
        meta: { ...offer.claimed, origin: offer.origin, path: offer.path },
      })
      pipe(near, far)
    },
    { ...opts, origins: opts.origins ?? sameOrigin() },
  )
