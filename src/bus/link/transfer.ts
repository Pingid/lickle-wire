import type { Port, Unsub } from '../../core/index.ts'
import type { Accept, Link } from './index.ts'

/**
 * Moving a connection, rather than copying a value.
 *
 * `Port.send` takes one argument and always has: a transfer list as a second
 * parameter would put "can this transport hand over ownership?" into every
 * signature that mentions a port, for the one family of transports that can.
 * So it rides in the message instead, and the capability becomes a type — a
 * {@link TransferPort} is a `Port` whose frames happen to carry channels.
 *
 * The two directions are different frames, not one shared shape. Outbound you
 * say what you are giving away; inbound you are told what you were given and
 * where it came from. Origin belongs only on the second, and is per-message
 * rather than per-connection because several senders share one target: a page
 * with three frames posts to one `window`.
 */

export declare namespace Transfer {
  /** Outbound: the message, plus what you are handing over with it. */
  export interface Out<S, C> {
    readonly data: S
    readonly transfer?: readonly C[] | undefined
  }

  /** Inbound: the message, what you were handed, and where it came from. */
  export interface In<R, C> {
    readonly data: R
    readonly transfer: readonly C[]
    /** `''` when the platform has no notion of one. */
    readonly origin: string
  }

  /**
   * The frame an offer travels as. Carries `kind`, never `$` — a protocol
   * envelope carries `$`/`v`, so the two cannot be mistaken for each other on
   * a shared wire.
   */
  export interface Open {
    readonly kind: typeof OPEN
    readonly v: 1
    readonly name: string
    /** Hops it was relayed through, nearest first. */
    readonly path: readonly string[]
    /** What the peer says about itself. Untrusted. */
    readonly meta?: Link.Meta | undefined
  }

  /** A transfer port carrying handshake offers. */
  export interface Offers<C> extends TransferPort<Open, Open, C> {}
}

/**
 * A port that can move connections, not just values.
 *
 * `C` is whatever the platform calls one: a `MessagePort` in a page, a
 * `runtime.Port` in an extension, a socket on a server. Nothing here looks
 * inside it.
 */
export interface TransferPort<S, R = S, C = unknown> extends Port<Transfer.Out<S, C>, Transfer.In<R, C>> {
  transfers?: true
}

const OPEN = 'wire/open'

const isOpen = (d: unknown): d is Transfer.Open => {
  if (typeof d !== 'object' || d === null) return false
  const o = d as Partial<Transfer.Open>
  return o.kind === OPEN && o.v === 1 && typeof o.name === 'string' && Array.isArray(o.path)
}

/**
 * Hand a channel up towards the hub as an offer.
 *
 * Nothing waits for an answer. An unclaimed channel simply never carries a
 * greeting, and `persistent` gives up on its own timeout and mints a fresh one
 * — which is the only reason a new channel per attempt matters.
 */
export const postOffer = <C>(up: Transfer.Offers<C>, name: string, channel: C, meta?: Link.Meta): void =>
  up.send({ data: { kind: OPEN, v: 1, name, path: [], meta }, transfer: [channel] })

/**
 * Read offers off a transfer port, shaped for {@link accept}.
 *
 * Everything the policy needs comes off the frame: the peer's claims from
 * `data`, the channel from `transfer`, and the provenance from `origin`. A
 * frame that is not an offer, or carries no channel, is not ours — a mux or a
 * hub may share this wire.
 */
export const readOffers =
  <C>(up: Transfer.Offers<C>) =>
  (take: (offer: Accept.Offer<C>) => void): Unsub =>
    up.listen((msg) => {
      if (!isOpen(msg.data)) return
      const channel = msg.transfer[0]
      if (channel === undefined) return
      take({
        name: msg.data.name,
        origin: msg.origin,
        path: msg.data.path,
        claimed: msg.data.meta,
        channel,
      })
    })

/**
 * Pass an offer one hop on, shaped for {@link relay}.
 *
 * The frame is rebuilt rather than forwarded: spreading what arrived would
 * pass any object of the right shape, from any origin, straight through.
 * `relay` has already recorded this hop in `path`.
 */
export const passOffer =
  <C>(up: Transfer.Offers<C>) =>
  (offer: Accept.Offer<C>): void =>
    up.send({
      data: { kind: OPEN, v: 1, name: offer.name, path: offer.path, meta: offer.claimed },
      transfer: [offer.channel],
    })
