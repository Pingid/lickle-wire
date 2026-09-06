/**
 * Layer 1 — framing.
 *
 * Frames are deliberately untyped in the payload. Payload types live at the
 * `Topic<D>` boundary, which is the only place a caller supplies or consumes
 * one; threading `keyof T` through the wire types buys nothing but casts.
 *
 * `ready` is the only frame a hub originates on its own. Everything above needs
 * to tell "connected and idle" apart from "never connected", and data traffic
 * cannot do that: a publish-only peer and a subscriber to a quiet channel look
 * exactly like a dead port. It also gives layer 0 a guarantee it can lean on —
 * on a healthy connection the first inbound message arrives promptly and
 * unprompted — which is how `Link.persistent` decides an attempt succeeded.
 *
 *
 * There is still no ACK. Delivery confirmation is a separate concern from
 * routing; build it on top with a correlation id and `Topic.send(p, { to })`.
 */

import type { Validate } from '../core/index.ts'

/**
 * Payload types keyed by channel. `unknown` rather than `any`, so an
 * unparameterised `Topics` still type-checks payloads instead of silently
 * disabling every check downstream.
 */
export type Topics = Record<string, any>

/**
 * Bumped on any wire-incompatible change.
 *
 * 2 — `ready` carries the peer id; `data` carries `to`, `from` and `r`.
 * 1 — initial.
 */
export const VERSION = 2

export type Frame =
  /** Hub to peer, unprompted, once per accepted connection. */
  | { t: 'ready'; id: string }
  | { t: 'sub'; c: string }
  | { t: 'unsub'; c: string }
  | {
      t: 'data'
      c: string
      d: unknown
      /** Peer to hub: deliver only to this peer id, ignoring subscriptions. */
      to?: string
      /** Hub to peer: the peer id that published it. Absent means the hub itself. */
      from?: string
      /** Hub to peer: a retained value replayed on subscribe, not live traffic. */
      r?: true
    }

/** `$` tags the owning hub, so several hubs can share one runtime. */
export type Envelope = Frame & { $: string; v: number }

export type Unsealed =
  | { readonly ok: true; readonly frame: Frame }
  /**
   * `foreign` — another hub's traffic, or not ours at all. Expected and silent.
   * `version` — our tag, an incompatible wire. Reported, because extensions and
   * tabs reload one context at a time, so a stale peer retrying against a hub
   * that will never understand it is routine.
   * `malformed` — our tag and version, but not a frame.
   */
  | {
      readonly ok: false
      readonly reason: 'foreign' | 'version' | 'malformed'
      readonly version?: number | undefined
    }

export type Validators<T extends Topics> = {
  readonly [K in keyof T & string]?: Validate<unknown, T[K]>
}

export type ProtocolFault = 'version' | 'malformed' | 'payload'

export const seal = ($: string, f: Frame): Envelope => ({ ...f, $, v: VERSION })

/**
 * The single place that decides "is this ours", and the only narrowing point.
 * Produces a real discriminated union, so every consumer is an exhaustive
 * `switch` rather than a hand-rolled dispatch table.
 */
export const unseal = ($: string, m: unknown): Unsealed => {
  if (typeof m !== 'object' || m === null) return bad('foreign')
  const e = m as Partial<Envelope>
  if (e.$ !== $) return bad('foreign')
  if (e.v !== VERSION) return bad('version', typeof e.v === 'number' ? e.v : undefined)
  switch (e.t) {
    case 'ready':
      return typeof e.id === 'string' ? { ok: true, frame: { t: 'ready', id: e.id } } : bad('malformed')
    case 'sub':
    case 'unsub':
      return typeof e.c === 'string' ? { ok: true, frame: { t: e.t, c: e.c } } : bad('malformed')
    case 'data': {
      if (typeof e.c !== 'string') return bad('malformed')
      const f: Frame = { t: 'data', c: e.c, d: e.d }
      if (typeof e.to === 'string') f.to = e.to
      if (typeof e.from === 'string') f.from = e.from
      if (e.r === true) f.r = true
      return { ok: true, frame: f }
    }
    default:
      return bad('malformed')
  }
}

const bad = (reason: 'foreign' | 'version' | 'malformed', version?: number): Unsealed => ({
  ok: false,
  reason,
  version,
})

/** Carries enough to act on: which hub, which channel, which peer. */
export class ProtocolError extends Error {
  constructor(
    readonly fault: ProtocolFault,
    message: string,
    readonly detail: {
      hub: string
      channel?: string | undefined
      peer?: string | undefined
      version?: number | undefined
    } = { hub: '' },
  ) {
    super(message)
    this.name = 'ProtocolError'
  }
}
