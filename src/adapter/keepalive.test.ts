import { describe, expect, it, vi } from 'vitest'
import { fakeClock } from '../bus/testing.ts'
import { keepalive } from './keepalive.ts'

const ping = (id: number) => ({ kind: 'wire/ctrl', c: 'ping', id })
const pong = (id: number) => ({ kind: 'wire/ctrl', c: 'pong', id })

const rig = (opts: Parameters<typeof keepalive>[2] = {}) => {
  const clock = fakeClock()
  const sent: unknown[] = []
  const dead = vi.fn()
  const beat = keepalive(
    (m) => {
      sent.push(m)
      return true
    },
    dead,
    { clock, ...opts },
  )
  return { beat, clock, sent, dead }
}

describe('keepalive', () => {
  it('probes on the interval and waits for the matching pong', () => {
    const { beat, clock, sent, dead } = rig({ interval: 100, timeout: 50 })
    beat.start()
    expect(sent).toEqual([])

    clock.advance(100)
    expect(sent).toEqual([ping(1)])

    // A pong for a probe we already gave up on says nothing about this one.
    expect(beat.inbound(pong(99))).toBe(true)
    clock.advance(49)
    expect(dead).not.toHaveBeenCalled()

    expect(beat.inbound(pong(1))).toBe(true)
    clock.advance(100)
    expect(sent).toEqual([ping(1), ping(2)])
    expect(dead).not.toHaveBeenCalled()
  })

  it('declares the peer dead when the pong misses its deadline', () => {
    const { beat, clock, dead } = rig({ interval: 100, timeout: 50 })
    beat.start()
    clock.advance(150)
    expect(dead).toHaveBeenCalledTimes(1)
    // Nothing is left armed to fire again.
    expect(clock.pending).toBe(0)
  })

  it('with timeout 0 keeps pinging on the interval and arms exactly one timer', () => {
    const { beat, clock, sent, dead } = rig({ interval: 100, timeout: 0 })
    beat.start()
    clock.advance(300)
    expect(sent).toEqual([ping(1), ping(2), ping(3)])
    expect(clock.pending).toBe(1)
    expect(dead).not.toHaveBeenCalled()
  })

  it('with interval 0 never probes but still answers, because liveness is symmetric', () => {
    const { beat, clock, sent, dead } = rig({ interval: 0 })
    beat.start()
    expect(clock.pending).toBe(0)
    clock.advance(10_000)
    expect(sent).toEqual([])

    expect(beat.inbound(ping(7))).toBe(true)
    expect(sent).toEqual([pong(7)])
    expect(dead).not.toHaveBeenCalled()
  })

  it('start is idempotent, so a second call cannot double-arm', () => {
    const { beat, clock, sent } = rig({ interval: 100, timeout: 50 })
    beat.start()
    beat.start()
    expect(clock.pending).toBe(1)
    clock.advance(100)
    expect(sent).toEqual([ping(1)])
  })

  it('passes payload through and consumes only control traffic', () => {
    const { beat } = rig()
    expect(beat.inbound({ $: 'h', v: 2, t: 'ready', id: 'p1' })).toBe(false)
    expect(beat.inbound('hello')).toBe(false)
    expect(beat.inbound(null)).toBe(false)
    expect(beat.inbound({ kind: 'wire/open', v: 1 })).toBe(false)
  })

  it('bye is terminal, and farewell sends one', () => {
    const { beat, sent, dead } = rig()
    beat.farewell()
    expect(sent).toEqual([{ kind: 'wire/ctrl', c: 'bye' }])

    expect(beat.inbound({ kind: 'wire/ctrl', c: 'bye' })).toBe(true)
    expect(dead).toHaveBeenCalledTimes(1)
  })

  it('stop is terminal and idempotent', () => {
    const { beat, clock, sent, dead } = rig({ interval: 100, timeout: 50 })
    beat.start()
    beat.stop()
    beat.stop()
    expect(clock.pending).toBe(0)
    clock.advance(10_000)
    expect(sent).toEqual([])
    expect(dead).not.toHaveBeenCalled()

    // Stopped for good: `start` after `stop` arms nothing.
    beat.start()
    expect(clock.pending).toBe(0)
  })
})
