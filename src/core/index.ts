export * from './validate.ts'

/**
 * The one contract every layer speaks.
 *
 * A `Port` is one end of a duplex message channel: an `ILink` is a port, a
 * `Topic` is a port, and RPC and replication run over any port. Keeping it to
 * two members is what lets a `Worker`, a `MessagePort`, a hub topic and a
 * hand-rolled test double all plug into the same code without adapters.
 */

export type Unsub = () => void

export interface ListenOptions {
  /**
   * Detach when aborted; an alternative to holding the returned teardown. Both
   * are safe to use at once and safe to use twice.
   */
  signal?: AbortSignal | undefined
  /**
   * Terminal. Called at most once, when nothing more will be delivered. `error`
   * is set when the port failed and undefined when it closed cleanly. No `next`
   * follows it.
   */
  onClose?: ((error?: unknown) => void) | undefined
}

/**
 * Implementations must (1) iterate a copy of their listeners, so a listener may
 * detach from inside its own callback, and (2) invoke `onClose` synchronously
 * from `listen` when already closed. Consumers that attach lazily and release
 * on the last subscriber rely on both.
 */
export interface Port<S, R = S> {
  /** Write a value to the far side. Never throws. */
  send(value: S): void
  /** Read values from the far side. Returns a teardown. */
  listen(next: (value: R) => void, opts?: ListenOptions): Unsub
}

/** Injected so backoff and heartbeats are testable without fake globals. */
export interface Clock {
  now(): number
  /** Returns a cancel function. */
  timer(fn: () => void, ms: number): () => void
}

export const systemClock: Clock = {
  now: () => Date.now(),
  timer: (fn, ms) => {
    const id = setTimeout(fn, ms)
    return () => clearTimeout(id)
  },
}
