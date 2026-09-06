/* -------------------------------------------------------------------------- */
/* link core                                                                  */
/* -------------------------------------------------------------------------- */

import { attempt, emitter, noop, report, type Emitter } from '../../core/internal.ts'
import type { ListenOptions, Unsub } from '../../core/index.ts'
import type { ILink } from './index.ts'

export interface BaseLink<T> {
  /** False once `shut` has run. Distinct from the public `up`. */
  readonly alive: boolean
  /** Inbound from the transport. Buffered if nobody is listening yet. */
  deliver(msg: T): void
  /** Emit an `up` transition without tearing the link down. */
  signal(up: boolean): void
  /** Terminal and idempotent. */
  shut(): void
  /** Build the public link. `send` is only reached while alive. */
  expose(send: (msg: T) => boolean, close: () => void): ILink<T>
}

export declare namespace BaseLink {
  export interface Options<T> {
    /** Read on every access, because a persistent link's far end changes. */
    describe(): { remote: string; meta: ILink.Meta }
    /**
     * Bound on inbound messages held before the first `listen`. Dropping these
     * loses the hub's greeting, which is the one message that must not be lost.
     * 0 disables buffering.
     */
    pending?: number | undefined
    /** Overrides the public `up`. Defaults to "not shut". */
    up?: (() => boolean) | undefined
    onError?: ((err: unknown) => void) | undefined
    onDrop?: ((msg: T) => void) | undefined
    /** Runs once, on the first `shut`. Detach transport handlers here. */
    release?: (() => void) | undefined
  }
}

export const baseLink = <T>(o: BaseLink.Options<T>): BaseLink<T> => {
  const cap = o.pending ?? 64
  const listeners = emitter<[T]>(o.onError)
  const changes = emitter<[boolean]>(o.onError)
  const gone = emitter<[]>(o.onError)
  const pending: T[] = []
  let alive = true
  // Seeded from the override so a link that starts down reports its first `true`.
  let up = o.up ? o.up() : true

  const shut = () => {
    if (!alive) return
    alive = false
    if (o.release) attempt(o.release, undefined, o.onError)
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

  return {
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
          const dropped = pending.shift() as T
          o.onDrop?.(dropped)
        }
        pending.push(msg)
        return
      }
      listeners.emit(msg)
    },
    signal(next) {
      if (next === up) return
      up = next
      changes.emit(next)
    },
    shut,
    expose(send, close) {
      return {
        get remote() {
          return o.describe().remote
        },
        get meta() {
          return o.describe().meta
        },
        get up() {
          return o.up ? o.up() : alive
        },
        changed: (fn, opts) => on(changes, fn, opts),
        listen: (fn, opts) => {
          const off = on(listeners, fn, opts)
          if (pending.length > 0) {
            for (const m of pending.splice(0)) {
              try {
                fn(m)
              } catch (err) {
                report(err, o.onError)
              }
            }
          }
          return off
        },
        closed: (fn, opts) => {
          if (!alive) {
            fn()
            return noop
          }
          return gone.add(fn, opts?.signal)
        },
        send: (msg) => (alive ? send(msg) : false),
        close,
      }
    },
  }
}
