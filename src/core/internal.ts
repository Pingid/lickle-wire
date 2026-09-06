import type { Clock } from './index.ts'

export * from './validate.ts'

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

/* -------------------------------------------------------------------------- */
/* queue                                                                      */
/* -------------------------------------------------------------------------- */

export interface QueueOptions {
  /** Values held while the consumer is slow; the oldest is dropped past it. Default unbounded. */
  buffer?: number | undefined
  /**
   * Runs once, only when the consumer abandons early — `return()` or `throw()`
   * before the producer called `end()` or `fail()`. The pull-model analogue of
   * "unsubscribe before complete".
   */
  onReturn?: (() => void) | undefined
}

export interface Queue<T> {
  /** Ignored once ended, failed or abandoned. */
  push(value: T): void
  /** Producer finished: buffered values drain first, then `done`. */
  end(): void
  /** Producer failed: buffered values drain first, then the next pull rejects once, then `done`. */
  fail(error: unknown): void
  readonly iterator: QueueIterator<T>
}

/** An async iterator whose `return` and `throw` are always present. */
export interface QueueIterator<T> extends AsyncIterableIterator<T> {
  return(value?: unknown): Promise<IteratorResult<T>>
  throw(error?: unknown): Promise<IteratorResult<T>>
}

type Pull<T> = { resolve: (r: IteratorResult<T>) => void; reject: (e: unknown) => void }
type Terminal = { failed: false } | { failed: true; error: unknown }

/** A push-to-pull buffer: what turns a stream of callbacks into a `for await`. */
export const queue = <T>(opts: QueueOptions = {}): Queue<T> => {
  const cap = opts.buffer ?? Infinity
  const values: T[] = []
  const pulls: Pull<T>[] = []
  let terminal: Terminal | null = null

  /** Hand one waiter the next thing — a value, the error (once), or done. False if nothing is ready. */
  const take = (p: Pull<T>): boolean => {
    if (values.length > 0) {
      p.resolve({ value: values.shift() as T, done: false })
      return true
    }
    if (!terminal) return false
    if (terminal.failed) {
      const e = terminal.error
      terminal = { failed: false }
      p.reject(e)
      return true
    }
    p.resolve({ value: undefined, done: true })
    return true
  }
  const settle = () => {
    while (pulls.length > 0) {
      if (!take(pulls[0] as Pull<T>)) return
      pulls.shift()
    }
  }
  const close = (t: Terminal, early: boolean) => {
    if (terminal) return
    // The consumer walked away: nothing buffered will be read.
    if (early) values.length = 0
    terminal = t
    settle()
    if (early) opts.onReturn?.()
  }

  const iterator: QueueIterator<T> = {
    [Symbol.asyncIterator]() {
      return iterator
    },
    next: () =>
      new Promise<IteratorResult<T>>((resolve, reject) => {
        const p = { resolve, reject }
        if (!take(p)) pulls.push(p)
      }),
    return: (value?: unknown) => {
      close({ failed: false }, true)
      return Promise.resolve({ value: value as T, done: true })
    },
    throw: (error?: unknown) => {
      close({ failed: true, error }, true)
      return Promise.reject(error)
    },
  }

  return {
    iterator,
    push(value) {
      if (terminal) return
      const p = pulls.shift()
      if (p) {
        p.resolve({ value, done: false })
        return
      }
      if (values.length >= cap) values.shift()
      values.push(value)
    },
    end: () => close({ failed: false }, false),
    fail: (error) => close({ failed: true, error }, false),
  }
}
