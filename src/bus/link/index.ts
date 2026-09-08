import type { ListenOptions, Port, Unsub } from '../../core/index.ts'
import type { Transfer } from './transfer.ts'

export * from './base.ts'
export * from './accept.ts'
export * from './transfer.ts'
export * from './outbox.ts'
export * from './slot.ts'
export * from './mux.ts'
export * from './source.ts'
export * from './persistent.ts'
export * from './pair.ts'

/**
 * Layer 0 — transport.
 *
 * A `Link` is one end of an open duplex message channel. It knows nothing about
 * hubs, channels, subscriptions or framing. Everything above this file is
 * testable without `chrome` and without a DOM.
 *
 * Reconnection lives here too, as `persistent`. It is a transport concern, and
 * keeping it out of `Session` leaves the session as an intent set plus a codec
 * and makes the retry policy testable on its own with a fake clock.
 *
 * Putting wire on a transport it does not ship with is {@link defineLink} and
 * {@link defineSource}, whose types live in the {@link Link} namespace below.
 * `@lickle/wire/browser` is written against those and nothing else.
 */

export interface Link<S = unknown, R = S> extends Port<S, R> {
  /** Label for the far end. Adapter-defined, not unique, for logs and policy. */
  readonly remote: string
  /**
   * Per-connection facts from the adapter: a chrome `MessageSender`, a socket
   * URL, a tab id. Per-connection rather than per-message, because sender
   * identity does not change between frames.
   */
  readonly meta: Link.Meta
  /** True while `send` can actually reach the far end. */
  readonly up: boolean
  /**
   * Fires on every `up` transition. Read `up` for the current value.
   *
   * Every subscription here takes `ListenOptions` as well as returning a
   * teardown, so a caller can hang the whole tree off one `AbortSignal`
   * instead of collecting arrays of closures.
   */
  changed(fn: (up: boolean) => void, opts?: ListenOptions): Unsub
  /** Never throws. Returns false if the message was dropped. */
  send(msg: S): boolean
  /** Inbound messages that arrive before the first listener are buffered. */
  listen(fn: (msg: R) => void, opts?: ListenOptions): Unsub
  /** Terminal. Fires immediately if the link is already closed. */
  closed(fn: () => void, opts?: ListenOptions): Unsub
  close(): void
}

export declare namespace Link {
  /**
   * Per-connection facts from the adapter: a socket URL, an origin, a tab id.
   *
   * Open by design — meta is where an adapter puts what only it knows — but the
   * keys this package writes are named, so `peer.meta.path` is checked instead
   * of hoped for. `unknown`, not `any`: reading someone else's key should cost
   * a check.
   */
  export interface Meta {
    /** Origin the connection was accepted from. `''` for a worker's owner. */
    readonly origin?: string
    /** Origins the handshake was relayed through, nearest frame first. */
    readonly path?: readonly string[]
    readonly [key: string]: unknown
  }
  /** Produces a fresh `Link`. Called again on each reconnect attempt. */
  export type Connector<S = unknown, R = S> = () => Link<S, R>
  /** Offers inbound `Link`s. Returns a teardown that closes what it produced. */
  export type Source<S = unknown, R = S> = (onLink: (link: Link<S, R>) => void) => Unsub
  /**
   * Anything a hub can serve: one link, or a source of them. A function is a
   * {@link Link.Source}; anything else is a single link.
   */
  export type Servable<S = unknown, R = S> = Link<S, R> | Source<S, R>

  /* ------------------------------------------------------------------------ */
  /* authoring                                                                */
  /* ------------------------------------------------------------------------ */

  /**
   * The inbound half, handed to {@link defineLink}'s `open` before there is a
   * link. Everything a transport can report — a frame arrived, the peer came
   * back, the peer is gone, something went wrong that is not fatal — and
   * nothing else.
   */
  export interface Host<T> {
    /** False once `shut` has run. Distinct from the public `up`: a link can be down and alive. */
    readonly alive: boolean
    /** Inbound from the transport. Held until the first `listen`. */
    deliver(msg: T): void
    /**
     * Move `up` and fire `changed`. Inert when nothing changes, so an adapter
     * may call it from every event without deduplicating first.
     */
    signal(up: boolean): void
    /** Terminal and idempotent, whoever calls it. Runs `release`, then fires `closed`. */
    shut(): void
    /**
     * A fault the link survives: an unclonable payload, a frame that would not
     * parse. Goes to `onError`, or is rethrown from a fresh task so it reaches
     * `window.onerror` instead of unwinding a transport callback and taking
     * unrelated listeners with it.
     */
    fail(err: unknown): void
  }

  /**
   * The outbound half, returned by {@link defineLink}'s `open`. Returned rather
   * than passed in so `release` closes over the handlers it has to detach — the
   * ordering every hand-written adapter gets wrong first.
   */
  export interface Transport<T> {
    /** Only reached while alive. False means the message was dropped. */
    send(msg: T): boolean
    /**
     * The consumer called `close()`. Say goodbye here if the protocol has one.
     * The link shuts immediately afterwards either way, so forgetting to is not
     * a leak.
     */
    close?(): void
    /** Runs once, on the first `shut`, whatever caused it. Detach here. */
    release?(): void
  }

  export interface Options<T = unknown> {
    /**
     * Label for the far end, for logs and policy. Not unique. A thunk when the
     * far end changes under the link, which is what `persistent` does on every
     * reconnect.
     */
    remote?: string | (() => string) | undefined
    /** Per-connection facts from the adapter. A thunk for the same reason as `remote`. */
    meta?: Meta | (() => Meta) | undefined
    /**
     * Bound on inbound messages held before the first `listen`. Dropping these
     * loses the hub's greeting, which is the one message that must not be lost.
     * 0 disables buffering.
     */
    pending?: number | undefined
    /**
     * Whether the link starts up. `false` for a transport that must connect
     * first, so its first `signal(true)` reads as a transition rather than a
     * repeat `changed` swallows.
     */
    up?: boolean | undefined
    onError?: ((err: unknown) => void) | undefined
    onDrop?: ((msg: T) => void) | undefined
  }

  /** The half of a {@link defineSource} handed to its `open`. */
  export interface SourceHost<S, R = S> {
    /**
     * Hand a link to whoever is serving. Closed instead of offered once the
     * source has stopped, so a link minted from an event in flight is never
     * orphaned.
     */
    offer(link: Link<S, R>): void
    /** A fault the source survives: a throwing filter, an offer that could not be built. */
    fail(err: unknown): void
    /**
     * Aborts when the source stops. Hand it to anything that takes one and the
     * source needs no teardown of its own.
     */
    readonly signal: AbortSignal
  }

  export interface SourceOptions {
    /** Stops the source when aborted, closing every link it produced. */
    signal?: AbortSignal | undefined
    onError?: ((err: unknown) => void) | undefined
  }

  export type Receiving<L> = L extends Link<any, infer R> ? R : never

  export type Sending<L> = L extends Link<infer S, any> ? S : never

  export interface TransferLink<S, R = S, C = unknown> extends Link<Transfer.Out<S, C>, Transfer.In<R, C>> {}
}
