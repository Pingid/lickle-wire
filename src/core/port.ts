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
  listen(next: (value: R) => void, opts?: ListenOptions): () => void
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

type EventListener<E extends Event = Event> = {
  addEventListener(type: string, listener: (event: E) => void): void
  removeEventListener(type: string, listener: (event: E) => void): void
}

type MessagePort = {
  postMessage(message: any, transfer: Transferable[]): void
  start?: () => void
}

type MessageTarget = EventListener<MessageEvent> & MessagePort

type IntoPort = MessageTarget | EventTarget

type Listener<T> = {
  next: (value: T) => void
  onClose?: (error?: unknown) => void
  signal?: AbortSignal
  abort?: () => void
  active: boolean
}

export function from<S = unknown>(target: MessageTarget): Port<S, MessageEvent<S>>

export function from(target: EventTarget): Port<Event>

export function from(target: IntoPort): Port<any, any> {
  const listeners = new Set<Listener<any>>()

  let attached = false
  let closed = (target as { closed?: boolean }).closed === true
  let closeError: unknown

  const emit = (value: any) => {
    // Snapshot is intentional: callbacks may detach themselves.
    for (const listener of [...listeners]) {
      if (closed) break
      if (!listener.active) continue

      listener.next(value)
    }
  }

  const finish = (error?: unknown) => {
    if (closed) return

    closed = true
    closeError = error

    detach()

    // Snapshot because onClose may itself perform teardown.
    for (const listener of [...listeners]) {
      if (!listener.active) continue

      listener.active = false

      if (listener.signal && listener.abort) {
        listener.signal.removeEventListener('abort', listener.abort)
      }

      listener.onClose?.(error)
    }

    listeners.clear()
  }

  const onMessage = (event: Event) => {
    emit(event)
  }

  const onClose = (event: Event) => {
    finish((event as Event & { error?: unknown }).error)
  }

  const onError = (event: Event) => {
    finish((event as ErrorEvent).error ?? event)
  }

  const attach = () => {
    if (attached || closed) return

    attached = true

    target.addEventListener('message', onMessage)
    target.addEventListener('close', onClose)
    target.addEventListener('error', onError)

    if ('start' in target) {
      target.start?.()
    }
  }

  function detach() {
    if (!attached) return

    attached = false

    target.removeEventListener('message', onMessage)
    target.removeEventListener('close', onClose)
    target.removeEventListener('error', onError)
  }

  const listen: Port<any, any>['listen'] = (next, opts = {}) => {
    if (closed) {
      // Required to be synchronous.
      opts.onClose?.(closeError)
      return noop
    }

    if (opts.signal?.aborted) {
      return noop
    }

    const listener: Listener<any> = {
      next,
      onClose: opts.onClose,
      signal: opts.signal,
      active: true,
    }

    const teardown = () => {
      if (!listener.active) return

      listener.active = false
      listeners.delete(listener)

      if (listener.signal && listener.abort) {
        listener.signal.removeEventListener('abort', listener.abort)
      }

      if (listeners.size === 0) {
        detach()
      }
    }

    if (opts.signal) {
      listener.abort = teardown
      opts.signal.addEventListener('abort', teardown, { once: true })
    }

    listeners.add(listener)

    if (listeners.size === 1) {
      attach()
    }

    return teardown
  }

  if ('postMessage' in target) {
    return {
      send(value) {
        try {
          target.postMessage(value, [])
        } catch {
          // Port.send must never throw.
        }
      },
      listen,
    }
  }

  return {
    send(value: Event) {
      try {
        target.dispatchEvent(value)
      } catch {
        // Port.send must never throw.
      }
    },
    listen,
  }
}

const noop = () => {}
