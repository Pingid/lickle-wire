import type { Port, Unsub } from '../../core/index.ts'

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
 */

export interface ILink<T = unknown> extends Port.Port<T, T> {
  /** Label for the far end. Adapter-defined, not unique, for logs and policy. */
  readonly remote: string
  /**
   * Per-connection facts from the adapter: a chrome `MessageSender`, a socket
   * URL, a tab id. Per-connection rather than per-message, because sender
   * identity does not change between frames.
   */
  readonly meta: ILink.Meta
  /** True while `send` can actually reach the far end. */
  readonly up: boolean
  /**
   * Fires on every `up` transition. Read `up` for the current value.
   *
   * Every subscription here takes `ListenOptions` as well as returning a
   * teardown, so a caller can hang the whole tree off one `AbortSignal`
   * instead of collecting arrays of closures.
   */
  changed(fn: (up: boolean) => void, opts?: Port.ListenOptions): Unsub
  /** Never throws. Returns false if the message was dropped. */
  send(msg: T): boolean
  /** Inbound messages that arrive before the first listener are buffered. */
  listen(fn: (msg: T) => void, opts?: Port.ListenOptions): Unsub
  /** Terminal. Fires immediately if the link is already closed. */
  closed(fn: () => void, opts?: Port.ListenOptions): Unsub
  close(): void
}

export declare namespace ILink {
  /**
   * Per-connection facts from the adapter: a socket URL, an origin, a tab id.
   */
  export type Meta = Readonly<Record<string, any>>
  /** Produces a fresh `Link`. Called again on each reconnect attempt. */
  export type Connector<T = unknown> = () => ILink<T>
  /** Offers inbound `Link`s. Returns a teardown that closes what it produced. */
  export type Source<T = unknown> = (onLink: (link: ILink<T>) => void) => Unsub
  /**
   * Anything a hub can serve: one link, or a source of them. A function is a
   * {@link ILink.Source}; anything else is a single link.
   */
  export type Servable<T = unknown> = ILink<T> | Source<T>
}
