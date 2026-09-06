import type { Clock } from '../core/index.ts'

/**
 * Test helpers. Nothing here is needed at runtime; it ships as a separate
 * entry point so a test suite can drive backoff, heartbeats and replication
 * timers deterministically.
 */

export interface FakeClock extends Clock {
  /** Advance virtual time, firing every timer due on the way, in due order. */
  advance(ms: number): void
  /** Timers armed and not yet fired or cancelled. */
  readonly pending: number
}

/**
 * A `Clock` that only moves when told to. Timers fire in due order, ties in
 * arming order, and a timer armed from inside a callback fires within the same
 * `advance` if it falls due. `advance(0)` fires zero-delay timers.
 */
export const fakeClock = (start = 0): FakeClock => {
  let now = start
  let seq = 0
  type Timer = { at: number; seq: number; fn: () => void }
  const timers: Timer[] = []
  return {
    now: () => now,
    timer(fn, ms) {
      const t: Timer = { at: now + Math.max(0, ms), seq: ++seq, fn }
      timers.push(t)
      return () => {
        const i = timers.indexOf(t)
        if (i >= 0) timers.splice(i, 1)
      }
    },
    advance(ms) {
      const until = now + Math.max(0, ms)
      for (;;) {
        let next: Timer | null = null
        for (const t of timers) {
          if (t.at > until) continue
          if (!next || t.at < next.at || (t.at === next.at && t.seq < next.seq)) next = t
        }
        if (!next) break
        timers.splice(timers.indexOf(next), 1)
        now = Math.max(now, next.at)
        next.fn()
      }
      now = until
    },
    get pending() {
      return timers.length
    },
  }
}
