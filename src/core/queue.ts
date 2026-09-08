/* -------------------------------------------------------------------------- */
/* queue                                                                      */
/* -------------------------------------------------------------------------- */

/**
 * A push-to-pull buffer: what turns a stream of callbacks into a `for await`.
 * Public because it is the bridge behind `Session.Topic.stream`, and a consumer
 * wanting the same over a raw link or an rpc side needs it without writing a
 * transport.
 */

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
