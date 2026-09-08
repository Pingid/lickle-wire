import type { Unsub } from './index.ts'

/**
 * Teardown bookkeeping, shared by everything that subscribes to a link.
 *
 * Both of these exist because of one rule in {@link Port}: a closed port
 * answers `onClose` synchronously from `listen`. That is deliberate — it is
 * what lets a consumer attach lazily and release on the last subscriber — but
 * it means a subscription can need tearing down before the variable holding it
 * has been assigned.
 */

export interface Detacher {
  /** Runs `off` immediately when `stop` has already run, rather than holding it. */
  add(off: Unsub): void
  /** Idempotent. Runs what was added, in the order it was added. */
  stop(): void
  readonly stopped: boolean
}

/**
 * A teardown set that cannot be beaten by its own contents.
 *
 * `closed` fires synchronously on a link that is already dead, so the second of
 * two subscriptions may be attached after the first has already torn the whole
 * consumer down. Every consumer of `Link` carried its own re-check for that;
 * this is the one place it lives.
 */
export const detacher = (): Detacher => {
  const offs: Unsub[] = []
  let stopped = false
  return {
    get stopped() {
      return stopped
    },
    add(off) {
      if (stopped) {
        off()
        return
      }
      offs.push(off)
    },
    stop() {
      if (stopped) return
      stopped = true
      for (const off of offs.splice(0)) off()
    },
  }
}

/**
 * Attach and detach as one expression, so the two cannot drift — the bug this
 * exists to prevent. The teardown is idempotent, and an `AbortSignal` is an
 * alternative to holding it. An already-aborted signal detaches before this
 * returns.
 */
export const bind = (attach: () => Unsub, signal?: AbortSignal): Unsub => {
  const detach = attach()
  let done = false
  const stop: Unsub = () => {
    if (done) return
    done = true
    signal?.removeEventListener('abort', stop)
    detach()
  }
  if (signal?.aborted) stop()
  else signal?.addEventListener('abort', stop, { once: true })
  return stop
}
