/**
 * Implementations must (1) iterate a copy of their listeners, so a listener may
 * detach from inside its own callback, and (2) invoke `onClose` synchronously
 * from `listen` when already closed. Consumers that attach lazily and release
 * on the last subscriber rely on both.
 */
export interface Port<S, R = S> extends Port.Send<S>, Port.Listen<R> {
  /** Write a value to the far side. Never throws. */
  send(value: S): void
  /** Read values from the far side. Returns a teardown. */
  listen(next: (value: R) => void, opts?: ListenOptions): () => void
}

export declare namespace Port {
  export interface Send<S> {
    send(value: S): void
  }

  export interface Listen<R> {
    listen(next: (value: R) => void, opts?: ListenOptions): () => void
  }
}

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
