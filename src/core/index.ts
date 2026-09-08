export * from './validate.ts'
export * from './teardown.ts'
export * from './queue.ts'

export type { ListenOptions, Port } from './port.ts'

/**
 * The one contract every layer speaks.
 *
 * A `Port` is one end of a duplex message channel: an `Link` is a port, a
 * `Topic` is a port, and RPC and replication run over any port. Keeping it to
 * two members is what lets a `Worker`, a `MessagePort`, a hub topic and a
 * hand-rolled test double all plug into the same code without adapters.
 */

export type Unsub = () => void

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
