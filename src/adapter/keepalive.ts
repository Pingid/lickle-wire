import { systemClock, type Clock, type Unsub } from '../core/index.ts'

/**
 * Liveness for transports that cannot report a disconnect.
 *
 * A `MessagePort` has no close event — a navigated frame or a terminated worker
 * leaves a port that looks perfectly healthy — so a probe is the only way
 * either side learns the peer is gone. Transports with a real disconnect event,
 * a `chrome.runtime.Port` or a `WebSocket`, do not import this: probing them
 * adds traffic and a second way to be wrong.
 *
 * A helper the adapter drives, not a wrapper around a link. A decorator would
 * mean two link cores and two pending buffers on every connection, to carry one
 * timer — and an adapter that never imports this carries no timer code at all.
 */

/**
 * Control traffic rides the same transport but never reaches `listen`, so
 * consumers above layer 0 never see it. Protocol envelopes carry `$`/`v` and
 * never `kind`, so the two cannot collide.
 */
const CTRL = 'wire/ctrl'

type Ctrl =
  | { readonly kind: typeof CTRL; readonly c: 'ping' | 'pong'; readonly id: number }
  | { readonly kind: typeof CTRL; readonly c: 'bye' }

const isCtrl = (d: unknown): d is Ctrl =>
  typeof d === 'object' && d !== null && (d as Ctrl).kind === CTRL && typeof (d as Ctrl).c === 'string'

export interface Keepalive {
  /** True when the frame was control traffic. Deliver anything else. */
  inbound(msg: unknown): boolean
  /** The orderly goodbye, so the far end does not wait out a heartbeat. */
  farewell(): void
  /** Arm the schedule. Idempotent. */
  start(): void
  /** Terminal and idempotent. Call it from `release`. */
  stop(): void
}

export declare namespace Keepalive {
  export interface Options {
    /**
     * ms between pings. 0 stops this side probing; the answering half still
     * replies, because liveness is symmetric and only one end may be configured
     * to ask.
     */
    interval?: number | undefined
    /** ms to wait for the pong before declaring the peer dead. 0 keeps pinging without a deadline. */
    timeout?: number | undefined
    clock?: Clock | undefined
  }
}

/**
 * `send` is the transport's own, not the link's: control frames must go out
 * over a link that is down, and must not be held in the pending buffer as if
 * they were payload. `onDead` is normally `host.shut`.
 */
export const keepalive = (
  send: (msg: unknown) => boolean,
  onDead: () => void,
  opts: Keepalive.Options = {},
): Keepalive => {
  const { interval = 5_000, timeout = 2_000, clock = systemClock } = opts
  let seq = 0
  let awaiting = 0
  let beat: Unsub | null = null
  let pong: Unsub | null = null
  let stopped = false

  const stop = () => {
    stopped = true
    beat?.()
    pong?.()
    beat = pong = null
  }

  const arm = () => {
    if (stopped || beat || interval <= 0) return
    beat = clock.timer(ping, interval)
  }

  const ping = () => {
    beat = null
    if (stopped) return
    const id = ++seq
    awaiting = id
    if (!send({ kind: CTRL, c: 'ping', id } satisfies Ctrl)) return
    // With a deadline the next beat is armed by the pong, so a silent peer is
    // probed once and then declared dead rather than pinged forever. Without
    // one there is nothing to wait for, so the schedule re-arms directly.
    if (timeout > 0) {
      pong = clock.timer(() => {
        if (awaiting !== id) return
        stop()
        onDead()
      }, timeout)
    } else arm()
  }

  const answer = (msg: Ctrl): boolean => {
    switch (msg.c) {
      case 'ping':
        send({ kind: CTRL, c: 'pong', id: msg.id } satisfies Ctrl)
        return true
      case 'pong':
        // A pong for a probe we already gave up on says nothing about the one
        // in flight.
        if (msg.id !== awaiting) return true
        awaiting = 0
        pong?.()
        pong = null
        if (timeout > 0) arm()
        return true
      case 'bye':
        stop()
        onDead()
        return true
    }
  }

  return {
    start: arm,
    stop,
    farewell: () => void send({ kind: CTRL, c: 'bye' } satisfies Ctrl),
    inbound: (msg) => isCtrl(msg) && answer(msg),
  }
}
