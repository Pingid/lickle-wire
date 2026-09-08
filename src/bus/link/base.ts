/* -------------------------------------------------------------------------- */
/* link core                                                                  */
/* -------------------------------------------------------------------------- */

import { attempt, emitter, noop, report, type Emitter } from '../../core/internal.ts'
import type { ListenOptions, Port, Unsub } from '../../core/index.ts'
import type { Link } from './index.ts'

/**
 * Every adapter needs the same bookkeeping: hold inbound until someone listens,
 * track `up` transitions, fire `closed` exactly once, go inert afterwards
 * rather than throw. Hand-written that is ~200 lines per transport and a fresh
 * set of ordering bugs each time. Written once here, an adapter is only the
 * part that is actually about its transport.
 */

const EMPTY: Link.Meta = Object.freeze({})

const thunk = <V>(v: V | (() => V) | undefined, fallback: V): (() => V) =>
  v === undefined ? () => fallback : typeof v === 'function' ? (v as () => V) : () => v

/**
 * An `Link` over an arbitrary transport.
 *
 * `open` runs before this returns: it is handed the inbound half and returns
 * the outbound half. It may `shut` synchronously — a transport handed over
 * already dead yields a link whose `closed` fires on subscribe, which is what
 * every consumer of `Link` is written against. A throw propagates, because an
 * adapter that could not attach has not produced a link; `persistent` reads
 * that as a failed attempt and retries.
 *
 * @example
 * ```ts
 * const link = defineLink<Msg>((host) => {
 *   const on = (e: MessageEvent) => host.deliver(e.data)
 *   sock.addEventListener('message', on)
 *   return { send: (m) => (sock.send(m), true), release: () => sock.removeEventListener('message', on) }
 * }, { remote: 'server' })
 * ```
 */
export const defineLink = <S, R = S>(
  open: (host: Link.Host<R>) => Link.Transport<S>,
  o: Link.Options<R> = {},
): Link<S, R> => {
  const cap = o.pending ?? 64
  const remote = thunk(o.remote, '')
  const meta = thunk(o.meta, EMPTY)
  const listeners = emitter<[R]>(o.onError)
  const changes = emitter<[boolean]>(o.onError)
  const gone = emitter<[]>(o.onError)
  const pending: R[] = []
  let alive = true
  let up = o.up ?? true
  let transport: Link.Transport<S> | null = null
  let released = false

  /** Idempotent, and correct before `open` has returned: `shut` may run from inside it. */
  const release = () => {
    if (released || !transport) return
    released = true
    if (transport.release) attempt(transport.release, undefined, o.onError)
  }

  const shut = () => {
    if (!alive) return
    alive = false
    release()
    if (up) {
      up = false
      changes.emit(false)
    }
    gone.emit()
    changes.clear()
    gone.clear()
    listeners.clear()
    pending.length = 0
  }

  /**
   * Shared by `changed` and `listen`: honours the signal, and wires `onClose`
   * to the link's own terminal event. A link close is always clean — failures
   * surface through `onError` first and then manifest as a close.
   */
  const on = <A extends unknown[]>(em: Emitter<A>, fn: (...a: A) => void, opts?: ListenOptions): Unsub => {
    if (!alive) {
      opts?.onClose?.()
      return noop
    }
    const off = em.add(fn, opts?.signal)
    const onClose = opts?.onClose
    if (!onClose) return off
    const offGone = gone.add(() => onClose(), opts.signal)
    return () => {
      off()
      offGone()
    }
  }

  const host: Link.Host<R> = {
    get alive() {
      return alive
    },
    deliver(msg) {
      if (!alive) return
      if (listeners.size === 0) {
        if (cap <= 0) {
          o.onDrop?.(msg)
          return
        }
        // `f?.(g())` skips `g()` when `f` is nullish, so the shift must not live in the argument.
        while (pending.length >= cap) {
          const dropped = pending.shift() as R
          o.onDrop?.(dropped)
        }
        pending.push(msg)
        return
      }
      listeners.emit(msg)
    },
    signal(next) {
      if (!alive || next === up) return
      up = next
      changes.emit(next)
    },
    shut,
    fail: (err) => report(err, o.onError),
  }

  transport = open(host)
  // `open` shut the link before handing anything back. Release what it built.
  if (!alive) release()

  return {
    get remote() {
      return remote()
    },
    get meta() {
      return meta()
    },
    get up() {
      return alive && up
    },
    changed: (fn, opts) => on(changes, fn, opts),
    listen: (fn, opts) => {
      const off = on(listeners, fn, opts)
      // A listener that arrived already aborted must not be handed the backlog:
      // `emitter.add` refused it, so nothing else would reach it either.
      if (pending.length === 0 || opts?.signal?.aborted) return off
      for (const m of pending.splice(0)) attempt(() => fn(m), undefined, o.onError)
      return off
    },
    closed: (fn, opts) => {
      if (!alive) {
        fn()
        return noop
      }
      return gone.add(fn, opts?.signal)
    },
    send: (msg) => (alive && transport !== null ? transport.send(msg) : false),
    close: () => {
      if (!alive) return
      if (transport?.close) attempt(transport.close, undefined, o.onError)
      shut()
    },
  }
}

/**
 * Lift a bare {@link Port} into a {@link Link}.
 *
 * A `Link` is already a `Port`, so the other direction is free; this is the way
 * back up. It is what lets anything that only ever needed two members — an rpc
 * side, a hub topic, a test double, a platform module that stopped at `Port` —
 * be served to a hub or handed to `persistent`.
 *
 * `up` is simply "not closed", because a `Port` has no liveness to report: the
 * terminal `onClose` becomes `closed`, and an error passed with it is reported
 * before the link shuts. A transport that knows more than that should use
 * {@link defineLink} directly and say so through `signal`.
 */
export const asLink = <S, R = S>(port: Port<S, R>, o: Link.Options<R> = {}): Link<S, R> =>
  defineLink<S, R>(
    (host) => ({
      send: (msg) => {
        port.send(msg)
        return true
      },
      // `listen` is attached here rather than lazily, because a link buffers
      // inbound from the moment it exists and a `Port` may drop what nobody
      // is listening for.
      release: port.listen((msg) => host.deliver(msg), {
        onClose: (err) => {
          if (err !== undefined) host.fail(err)
          host.shut()
        },
      }),
    }),
    o,
  )
