import type { Clock } from './index.ts'

export * from './validate.ts'
export * from './queue.ts'

type Unsub = () => void

/**
 * Not part of the public API.
 *
 * One implementation of the listener bookkeeping every transport needs, so the
 * adapters cannot drift apart in their teardown ordering, plus the small
 * scheduling and buffering helpers the layers above share.
 */

/**
 * A failure with no better home. Without a handler it is rethrown from a fresh
 * task, so it reaches `window.onerror` instead of unwinding a transport
 * callback and taking unrelated listeners with it.
 */
export const report = (err: unknown, onError?: (err: unknown) => void): void => {
  if (!onError) {
    queueMicrotask(() => {
      throw err
    })
    return
  }
  try {
    onError(err)
  } catch (nested) {
    queueMicrotask(() => {
      throw nested
    })
  }
}

/** Run untrusted user code — a policy hook, a watcher — without letting it escape. */
export const attempt = <V>(fn: () => V, fallback: V, onError?: (err: unknown) => void): V => {
  try {
    return fn()
  } catch (err) {
    report(err, onError)
    return fallback
  }
}

export const isPromise = <V>(v: V | Promise<V>): v is Promise<V> =>
  typeof (v as { then?: unknown } | null | undefined)?.then === 'function'

export const noop = (): void => {}

/* -------------------------------------------------------------------------- */
/* emitter                                                                    */
/* -------------------------------------------------------------------------- */

export interface Emitter<A extends unknown[]> {
  readonly size: number
  /**
   * `signal` is an alternative to holding the returned teardown; either
   * detaches. Both are safe to use at once and safe to use twice.
   */
  add(fn: (...a: A) => void, signal?: AbortSignal): Unsub
  /** One throwing listener never starves the rest. */
  emit(...a: A): void
  clear(): void
}

export const emitter = <A extends unknown[]>(onError?: (err: unknown) => void): Emitter<A> => {
  const ls = new Set<(...a: A) => void>()
  return {
    get size() {
      return ls.size
    },
    add(fn, signal) {
      if (signal?.aborted) return noop
      ls.add(fn)
      let done = false
      const off = () => {
        if (done) return
        done = true
        ls.delete(fn)
        signal?.removeEventListener('abort', off)
      }
      signal?.addEventListener('abort', off, { once: true })
      return off
    },
    emit(...a) {
      for (const fn of [...ls]) {
        try {
          fn(...a)
        } catch (err) {
          report(err, onError)
        }
      }
    },
    clear() {
      ls.clear()
    },
  }
}

/* -------------------------------------------------------------------------- */
/* repeat                                                                     */
/* -------------------------------------------------------------------------- */

export interface RepeatOptions {
  /** ms before the first call. `0` still defers to the clock: nothing fires synchronously. */
  delay: number
  /** ms between later calls. Omitted: one-shot. */
  period?: number | undefined
  /** Total calls before the schedule retires itself. Omitted: until cancelled. */
  count?: number | undefined
}

/**
 * `fn(0)` after `delay`, then `fn(1)`, `fn(2)`… every `period`, `count` times.
 *
 * The next tick is armed only after `fn` returns, and the schedule is marked
 * done before the last call, so a callback that reaches the returned teardown —
 * directly or through the caller's own state — is honoured rather than leaving
 * a dangling timer.
 */
export const repeat = (clock: Clock, { delay, period, count }: RepeatOptions, fn: (n: number) => void): Unsub => {
  let n = 0
  let done = false
  let cancel: Unsub | undefined
  const tick = () => {
    cancel = undefined
    const i = n++
    if (period === undefined || (count !== undefined && n >= count)) done = true
    fn(i)
    if (done || period === undefined) return
    cancel = clock.timer(tick, period)
  }
  cancel = clock.timer(tick, delay)
  return () => {
    done = true
    cancel?.()
    cancel = undefined
  }
}
