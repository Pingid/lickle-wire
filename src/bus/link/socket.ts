import { attempt, emitter } from '../../core/internal.ts'
import { defineLink } from './base.ts'
import { defineSource } from './source.ts'
import type { Link } from './index.ts'

/**
 * Anything with a wire and a hangup — enough of a socket to build a `Link` on.
 *
 * Declared with method shorthand rather than function-typed properties, so a
 * concrete socket's narrower `send` — a `string`, a `Bufferish`, whatever the
 * platform actually accepts — still satisfies this structurally instead of
 * being rejected for not taking `unknown`.
 */
export interface Socket<W = unknown> {
  send(data: W): unknown
  close(): unknown
}

export declare namespace SocketSource {
  export interface Options<S extends Socket, M = unknown> extends Link.SourceOptions {
    /** Label for the far end — an address, a session id. Not unique. `''` by default. */
    remote?: string | ((socket: S) => string) | undefined
    /** Per-connection facts, whatever the platform's socket carries. */
    meta?: ((socket: S) => Link.Meta) | undefined
    /** Defaults to JSON text frames. */
    encode?: ((msg: M) => Parameters<S['send']>[0]) | undefined
    decode?: ((data: unknown) => M) | undefined
  }
}

export interface SocketSource<S extends Socket, M = unknown> {
  /** A socket connected. Wire to the platform's own `open`/`connection` event. */
  open(socket: S): void
  /** A frame arrived. Wire to the platform's own `message` event. */
  message(socket: S, data: unknown): void
  /** A socket disconnected. Wire to the platform's own `close` event. */
  close(socket: S): void
  /**
   * A fault upstream of a socket — a bad frame, a throwing handler. Reported
   * even for a socket with no link, which is what a failed upgrade looks like.
   */
  error(socket: S, err: unknown): void
  /** Hand to `hub.serve(...)`. */
  readonly source: Link.Source<M>
}

/**
 * One `Link.Source` for every platform whose websocket API is a handful of
 * lifecycle callbacks rather than an event target: Bun's `.ws()`, `ws`'s
 * `WebSocketServer`, `Deno.upgradeWebSocket`, the browser's own `WebSocket`.
 *
 * The four methods are meant to be wired straight to whatever the platform
 * calls its socket events; correlating a socket with its `Link`, decoding
 * frames and tearing down on close all happen here, so each platform's own
 * adapter is only ever the handful of lines that differ.
 *
 * Events are relayed through an emitter rather than offered directly, because
 * a socket may connect before any hub is serving this source, or after one
 * stopped — routing through `defineSource`'s own subscribe/unsubscribe is what
 * makes both read as "nobody was listening" instead of a special case.
 *
 * @example
 * ```ts
 * const sockets = socketSource<WebSocket, Envelope>()
 * wss.on('connection', (ws) => {
 *   sockets.open(ws)
 *   ws.on('message', (data) => sockets.message(ws, data))
 *   ws.on('close', () => sockets.close(ws))
 *   ws.on('error', (err) => sockets.error(ws, err))
 * })
 * hub.serve(sockets.source)
 * ```
 */
export const socketSource = <S extends Socket, M = unknown>(
  opts: SocketSource.Options<S, M> = {},
): SocketSource<S, M> => {
  const encode = opts.encode ?? ((m: M) => JSON.stringify(m) as Parameters<S['send']>[0])
  const decode = opts.decode ?? ((d: unknown) => JSON.parse(String(d)) as M)
  const label = (s: S) => (typeof opts.remote === 'function' ? opts.remote(s) : (opts.remote ?? ''))

  type Event =
    | { kind: 'open'; socket: S }
    | { kind: 'message'; socket: S; data: unknown }
    | { kind: 'close'; socket: S }
    | { kind: 'error'; socket: S; data: unknown }
  const em = emitter<[Event]>(opts.onError)

  const source = defineSource<M>((host) => {
    const hosts = new WeakMap<object, Link.Host<M>>()

    return em.add((e) => {
      if (e.kind === 'open') {
        let link: Link<M>
        try {
          link = defineLink<M>(
            (h) => {
              hosts.set(e.socket, h)
              return {
                send: (msg) => attempt(() => (e.socket.send(encode(msg)), true), false, opts.onError),
                close: () => e.socket.close(),
                // Only its own entry: a second `open` for this socket must not
                // be unrouted by the first link closing.
                release: () => void (hosts.get(e.socket) === h && hosts.delete(e.socket)),
              }
            },
            { remote: label(e.socket), meta: opts.meta?.(e.socket), onError: opts.onError },
          )
        } catch (err) {
          // No link to offer, and the socket is ours: nobody else will close it.
          host.fail(err)
          attempt(() => e.socket.close(), undefined, opts.onError)
          return
        }
        host.offer(link)
        return
      }
      const h = hosts.get(e.socket)
      // A fault is worth hearing from a socket that never got a link: a failed
      // upgrade reports one before `open`, and swallowing it leaves no trace.
      if (e.kind === 'error') return (h ?? host).fail(e.data)
      if (!h) return
      if (e.kind === 'message') attempt(() => h.deliver(decode(e.data)), undefined, host.fail)
      else h.shut()
    })
  }, opts)

  return {
    open: (socket) => em.emit({ kind: 'open', socket }),
    message: (socket, data) => em.emit({ kind: 'message', socket, data }),
    close: (socket) => em.emit({ kind: 'close', socket }),
    error: (socket, err) => em.emit({ kind: 'error', socket, data: err }),
    source,
  }
}
