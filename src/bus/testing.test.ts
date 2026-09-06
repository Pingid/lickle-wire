import { describe, expect, it, vi } from 'vitest'
import { fakeClock } from './testing.js'

describe('fakeClock()', () => {
  it('fires timers in due order, ties in arming order', () => {
    const clock = fakeClock()
    const order: string[] = []
    clock.timer(() => order.push('a@20'), 20)
    clock.timer(() => order.push('b@10'), 10)
    clock.timer(() => order.push('c@20'), 20)
    clock.timer(() => order.push('d@30'), 30)
    clock.advance(20)
    expect(order).toEqual(['b@10', 'a@20', 'c@20'])
    expect(clock.pending).toBe(1)
    clock.advance(10)
    expect(order).toEqual(['b@10', 'a@20', 'c@20', 'd@30'])
    expect(clock.pending).toBe(0)
  })

  it('does not fire a timer before it is due', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    clock.timer(fn, 10)
    clock.advance(9)
    expect(fn).not.toHaveBeenCalled()
    clock.advance(1)
    expect(fn).toHaveBeenCalledOnce()
  })

  it('fires a timer armed inside a callback within the same advance when it falls due', () => {
    const clock = fakeClock()
    const order: string[] = []
    clock.timer(() => {
      order.push('outer')
      clock.timer(() => order.push('inner'), 5)
      clock.timer(() => order.push('late'), 50)
    }, 10)
    clock.advance(20)
    expect(order).toEqual(['outer', 'inner'])
    expect(clock.pending).toBe(1)
    clock.advance(40)
    expect(order).toEqual(['outer', 'inner', 'late'])
    expect(clock.pending).toBe(0)
  })

  it('cancel removes a timer and pending tracks the count', () => {
    const clock = fakeClock()
    const a = vi.fn()
    const b = vi.fn()
    const offA = clock.timer(a, 10)
    clock.timer(b, 10)
    expect(clock.pending).toBe(2)
    offA()
    expect(clock.pending).toBe(1)
    offA()
    expect(clock.pending).toBe(1)
    clock.advance(10)
    expect(a).not.toHaveBeenCalled()
    expect(b).toHaveBeenCalledOnce()
    expect(clock.pending).toBe(0)
  })

  it('cancelling a timer that already fired is harmless', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    const off = clock.timer(fn, 10)
    clock.timer(vi.fn(), 20)
    clock.advance(10)
    expect(fn).toHaveBeenCalledOnce()
    expect(() => off()).not.toThrow()
    expect(clock.pending).toBe(1)
  })

  it('a timer cancelled from inside an earlier callback does not fire', () => {
    const clock = fakeClock()
    const b = vi.fn()
    let offB = () => {}
    clock.timer(() => offB(), 10)
    offB = clock.timer(b, 10)
    clock.advance(10)
    expect(b).not.toHaveBeenCalled()
    expect(clock.pending).toBe(0)
  })

  it('advance(0) fires zero-delay timers, which never fire synchronously', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    clock.timer(fn, 0)
    expect(fn).not.toHaveBeenCalled()
    expect(clock.pending).toBe(1)
    clock.advance(0)
    expect(fn).toHaveBeenCalledOnce()
    expect(clock.pending).toBe(0)
  })

  it('clamps a negative delay to zero', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    clock.timer(fn, -5)
    clock.advance(0)
    expect(fn).toHaveBeenCalledOnce()
  })

  it('now() advances with the clock and reads the due time inside a callback', () => {
    const clock = fakeClock()
    expect(clock.now()).toBe(0)
    let seen = -1
    clock.timer(() => {
      seen = clock.now()
    }, 30)
    clock.advance(100)
    expect(seen).toBe(30)
    expect(clock.now()).toBe(100)
    clock.advance(0)
    expect(clock.now()).toBe(100)
  })

  it('starts at the given time', () => {
    const clock = fakeClock(500)
    expect(clock.now()).toBe(500)
    const fn = vi.fn()
    clock.timer(fn, 10)
    clock.advance(10)
    expect(fn).toHaveBeenCalledOnce()
    expect(clock.now()).toBe(510)
  })
})
