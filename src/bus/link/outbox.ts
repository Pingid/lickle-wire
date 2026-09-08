import { systemClock, type Clock } from '../../core/index.ts'

/**
 * Outbound messages held while a link is down.
 *
 * Shared by everything that outlives its transport — {@link persistent} across
 * a reconnect, {@link slot} between binds — because "what happens to a send
 * while nobody is there" is one policy, not two. Overflow is refused at the
 * call site rather than silently swallowed: `send` returns false and `onDrop`
 * fires, so a caller can tell.
 */

/** One outbound message waiting for the link to come up. */
type Held<T> = { msg: T; at: number }

export interface Outbox<T> {
  /** Hold a message until the link comes up. False when it was refused. */
  hold(msg: T): boolean
  /** Send everything held, oldest first. Whatever the link refuses is dropped. */
  drain(send: (msg: T) => boolean): void
  /** Drop everything held, reporting each. */
  clear(): void
  readonly size: number
}

export declare namespace Outbox {
  export interface Options<T> {
    /** How many messages may wait. 0 never buffers. */
    buffer?: number | undefined
    /**
     * ms a held message may wait before it is dropped. Without this, a send
     * during a long outage is delivered minutes later with no way for the
     * caller to have known. 0 is forever.
     */
    ttl?: number | undefined
    /** Which end of a full buffer loses. Defaults to refusing the new message. */
    overflow?: 'oldest' | 'newest' | undefined
    clock?: Clock | undefined
    /** A message that will never be delivered. */
    onDrop?: ((msg: T) => void) | undefined
  }
}

export const outbox = <T>(o: Outbox.Options<T> = {}): Outbox<T> => {
  const clock = o.clock ?? systemClock
  const cap = o.buffer ?? 64
  const ttl = o.ttl ?? 0
  const overflow = o.overflow ?? 'newest'
  const held: Held<T>[] = []

  /** Anything that waited longer than `ttl` is no longer worth delivering. */
  const expire = () => {
    if (ttl <= 0) return
    const cutoff = clock.now() - ttl
    while (held.length > 0 && (held[0] as Held<T>).at < cutoff) {
      const stale = held.shift() as Held<T>
      o.onDrop?.(stale.msg)
    }
  }

  return {
    get size() {
      return held.length
    },
    hold(msg) {
      if (cap <= 0) {
        o.onDrop?.(msg)
        return false
      }
      expire()
      if (held.length >= cap) {
        if (overflow === 'newest') {
          o.onDrop?.(msg)
          return false
        }
        while (held.length >= cap) {
          const oldest = held.shift() as Held<T>
          o.onDrop?.(oldest.msg)
        }
      }
      held.push({ msg, at: clock.now() })
      return true
    },
    drain(send) {
      expire()
      for (const one of held.splice(0)) if (!send(one.msg)) o.onDrop?.(one.msg)
    },
    clear() {
      for (const one of held.splice(0)) o.onDrop?.(one.msg)
    },
  }
}
