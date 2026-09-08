import { detacher, type Unsub } from '../../core/index.ts'
import { emitter, report } from '../../core/internal.ts'
import { defineLink } from './base.ts'
import { defineSource } from './source.ts'
import type { Link } from './index.ts'

/**
 * Many logical links over one real one.
 *
 * The handshake in `@lickle/wire/browser` moves a `MessagePort`, which is what
 * keeps a relaying ancestor out of the data path — but it only works where
 * ownership can be transferred. Everywhere else, and over any `Link` you
 * already hold, the way to let peers connect through a connection is to number
 * them and share it.
 *
 * That is the trade, and it is not hidden: a mux carries every channel's
 * traffic itself. Prefer a transferred port when you can have one.
 *
 * Buffering while the underlying link is down is deliberately not repeated
 * here: put a `persistent` or a `slot` underneath and it is handled once, for
 * every channel at once. Only payload is held that way — the frames that say
 * which channels exist are regenerated against whatever wire turns up, because
 * a replayed one could describe a peer that no longer exists.
 */

const MUX = 'wire/mux'

type Frame =
  | { readonly kind: typeof MUX; readonly c: number; readonly t: 'open'; readonly n: string; readonly m?: Link.Meta }
  | { readonly kind: typeof MUX; readonly c: number; readonly t: 'data'; readonly d: unknown }
  | { readonly kind: typeof MUX; readonly c: number; readonly t: 'close' }

const isFrame = (d: unknown): d is Frame =>
  typeof d === 'object' &&
  d !== null &&
  (d as Frame).kind === MUX &&
  typeof (d as Frame).c === 'number' &&
  typeof (d as Frame).t === 'string'

export interface Mux<S, R = S> {
  /**
   * Open a channel to the far side, where it surfaces on {@link incoming}.
   *
   * Nothing waits for an answer: the channel is usable at once, and its frames
   * queue behind the `open` on the same link, so they cannot overtake it.
   */
  open(name: string, opts?: { meta?: Link.Meta | undefined }): Link<S, R>
  /**
   * Channels the far side opened. Serve it to a hub, or subscribe to it.
   *
   * Channels that arrive before anything is listening are held, for the same
   * reason a link buffers its greeting: the first thing a peer says is the part
   * you cannot afford to drop.
   */
  readonly incoming: Link.Source<S, R>
  /** Channels open in either direction. */
  readonly size: number
  /**
   * Close every channel and stop reading the underlying link. Does not close
   * the link itself: the mux did not open it.
   */
  close(): void
}

export declare namespace Mux {
  /** The frame a mux puts on the wire. A link carrying one carries this. */
  export type Wire = Frame

  export interface Options {
    /**
     * Which half of the id space this end allocates from. The two ends must
     * disagree, the same way they must agree on a hub's name — an id chosen by
     * both at once is two channels wearing one number.
     */
    side: 'a' | 'b'
    /** Inbound channels held before anything serves {@link Mux.incoming}. */
    backlog?: number | undefined
    /** Inbound messages held on each channel before its first `listen`. */
    pending?: number | undefined
    onError?: ((err: unknown) => void) | undefined
  }
}

/**
 * Not public: one live channel, the handle that drives it, and — for one this
 * end opened — what it takes to announce it again on a fresh wire.
 */
type Channel<S, R = S> = {
  host: Link.Host<R>
  link: Link<S, R>
  readonly local: boolean
  readonly name: string
  readonly meta: Link.Meta | undefined
}

export const mux = <S, R = S>(link: Link<Mux.Wire>, opts: Mux.Options): Mux<S, R> => {
  const backlog = opts.backlog ?? 64
  const channels = new Map<number, Channel<S, R>>()
  const waiting: Link<S, R>[] = []
  const sinks = emitter<[Link<S, R>]>(opts.onError)
  const stops = detacher()

  // Odd for one end, even for the other. No negotiation, no round trip, and an
  // id arriving with our own parity is a misconfigured pair rather than a race.
  const mine = opts.side === 'a' ? 1 : 0
  let next = opts.side === 'a' ? 1 : 2
  let live = true

  const post = (f: Frame) => link.send(f)

  /**
   * Control frames are never held while the wire is down. A buffered `open`
   * would flush on reconnect *and* be re-announced, opening the same channel
   * twice; and one that reached a peer who has since been replaced means
   * nothing. `reannounce` is the single authority for what the far side knows.
   */
  const control = (f: Frame) => link.up && link.send(f)

  const channel = (id: number, remote: string, meta: Link.Meta, claimed?: Link.Meta): Link<S, R> => {
    let host!: Link.Host<R>
    const one = defineLink<S, R>(
      (h) => {
        host = h
        return {
          send: (d) => post({ kind: MUX, c: id, t: 'data', d }),
          // Tell the far side, if there is still a wire to tell it on.
          close: () => void control({ kind: MUX, c: id, t: 'close' }),
          release: () => channels.delete(id),
        }
      },
      {
        remote: () => remote,
        meta: () => meta,
        pending: opts.pending,
        // A channel is only as up as the link under it.
        up: link.up,
        onError: opts.onError,
      },
    )
    channels.set(id, {
      host: host!,
      link: one,
      local: id % 2 === mine % 2,
      name: remote,
      meta: claimed,
    })
    return one
  }

  const offer = (one: Link<S, R>) => {
    if (sinks.size > 0) {
      sinks.emit(one)
      return
    }
    if (waiting.length >= backlog) {
      // Refusing beats holding an unbounded queue of peers nobody asked for.
      report(new Error(`mux: inbound channel backlog of ${backlog} exceeded`), opts.onError)
      one.close()
      return
    }
    waiting.push(one)
  }

  const inbound = (f: Frame) => {
    const known = channels.get(f.c)
    switch (f.t) {
      case 'open': {
        if (f.c % 2 === mine % 2) {
          // Both ends allocating from one half: `side` is set the same on both.
          report(new Error(`mux: peer opened channel ${f.c} from this end's id space`), opts.onError)
          return
        }
        // A live id means the peer is announcing over a wire that came back —
        // it considers the channel continuous, we cannot, so the old one ends
        // and a fresh peer takes its place. The far side's session replays its
        // intent on the greeting, which is the same path a reconnect takes.
        if (known) known.host.shut()
        offer(channel(f.c, f.n, { ...f.m, via: link.remote }))
        return
      }
      case 'data':
        // An unknown id is a frame that crossed a close. Nothing to do.
        known?.host.deliver(f.d as R)
        return
      case 'close':
        known?.host.shut()
        return
    }
  }

  stops.add(
    link.listen((msg) => {
      if (isFrame(msg)) inbound(msg)
      // Anything else on this link is not ours; a mux may share it with a hub.
    }),
  )
  /**
   * The wire under us was replaced — a `slot` swapped, a `persistent` link
   * reconnected — so the far side is a mux that has never heard of our
   * channels. Announcing them again is what makes a mux usable over anything
   * that outlives its transport; without it every channel silently dead-ends
   * at a peer that will never answer.
   *
   * Announced before the channels are told they are up, so nothing a consumer
   * sends on that news can overtake its own `open`.
   */
  const reannounce = () => {
    for (const [id, c] of [...channels]) {
      if (c.local) control({ kind: MUX, c: id, t: 'open', n: c.name, m: c.meta })
    }
  }

  stops.add(
    link.changed((up) => {
      if (up) reannounce()
      for (const c of [...channels.values()]) c.host.signal(up)
    }),
  )
  // The wire is gone, so every channel on it is too — but each ends cleanly
  // rather than being told to send a `close` down a link that no longer exists.
  stops.add(link.closed(() => shut()))

  function shut() {
    if (!live) return
    live = false
    stops.stop()
    sinks.clear()
    for (const c of [...channels.values()]) c.host.shut()
    channels.clear()
    for (const one of waiting.splice(0)) one.close()
  }

  const incoming = defineSource<S, R>(
    (host) => {
      const off = sinks.add((one) => host.offer(one))
      for (const one of waiting.splice(0)) host.offer(one)
      return off
    },
    { onError: opts.onError },
  )

  return {
    incoming,
    get size() {
      return channels.size
    },
    open(name, o = {}) {
      if (!live) {
        // Hand back something already over rather than a channel to nowhere.
        const dead = defineLink<S, R>((host) => (host.shut(), { send: () => false }), { remote: name })
        return dead
      }
      const id = next
      next += 2
      const meta = o.meta ?? {}
      const one = channel(id, name, meta, o.meta)
      // Down right now means the far side does not exist yet; `reannounce`
      // introduces this channel to whichever one turns up.
      control({ kind: MUX, c: id, t: 'open', n: name, m: o.meta })
      return one
    },
    close: shut,
  }
}

/**
 * Join two links so each carries the other's traffic, and closing either ends
 * both. What a relay does when it cannot hand a connection on and has to stay
 * in the middle of it.
 */
export const pipe = <T>(a: Link<T>, b: Link<T>): Unsub => {
  const stops = detacher()
  const end = (other: Link<T>) => () => {
    stops.stop()
    other.close()
  }
  stops.add(a.listen((msg) => void b.send(msg)))
  stops.add(b.listen((msg) => void a.send(msg)))
  stops.add(a.closed(end(b)))
  stops.add(b.closed(end(a)))
  return stops.stop
}
