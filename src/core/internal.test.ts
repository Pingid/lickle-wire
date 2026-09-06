import { describe, expect, it, vi } from 'vitest'

import { emitter, queue, repeat } from './internal.ts'
import { fakeClock } from '../bus/testing.ts'
import type { Unsub } from '../index.ts'

describe('repeat()', () => {
  it('calls fn(0) after delay, then every period, count times, and retires', () => {
    const clock = fakeClock()
    const fn = vi.fn<(n: number) => void>()
    repeat(clock, { delay: 10, period: 5, count: 3 }, fn)
    clock.advance(9)
    expect(fn).not.toHaveBeenCalled()
    clock.advance(1)
    expect(fn.mock.calls.map(([n]) => n)).toEqual([0])
    clock.advance(5)
    expect(fn.mock.calls.map(([n]) => n)).toEqual([0, 1])
    clock.advance(5)
    expect(fn.mock.calls.map(([n]) => n)).toEqual([0, 1, 2])
    expect(clock.pending).toBe(0)
    clock.advance(100)
    expect(fn).toHaveBeenCalledTimes(3)
  })

  it('never fires synchronously, even with a zero delay', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    repeat(clock, { delay: 0 }, fn)
    expect(fn).not.toHaveBeenCalled()
    clock.advance(0)
    expect(fn).toHaveBeenCalledExactlyOnceWith(0)
  })

  it('runs until cancelled when count is omitted', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    const off = repeat(clock, { delay: 0, period: 10 }, fn)
    clock.advance(50)
    expect(fn).toHaveBeenCalledTimes(6)
    off()
    expect(clock.pending).toBe(0)
    clock.advance(100)
    expect(fn).toHaveBeenCalledTimes(6)
  })

  it('a callback that cancels from within stops the schedule', () => {
    const clock = fakeClock()
    let stop: Unsub = () => {}
    const fn = vi.fn((n: number) => {
      if (n === 1) stop()
    })
    stop = repeat(clock, { delay: 0, period: 10 }, fn)
    clock.advance(0)
    clock.advance(10)
    expect(fn.mock.calls.map(([n]) => n)).toEqual([0, 1])
    expect(clock.pending).toBe(0)
    clock.advance(100)
    expect(fn).toHaveBeenCalledTimes(2)
  })

  it('is a one-shot without a period', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    repeat(clock, { delay: 10 }, fn)
    clock.advance(10)
    expect(fn).toHaveBeenCalledExactlyOnceWith(0)
    expect(clock.pending).toBe(0)
    clock.advance(100)
    expect(fn).toHaveBeenCalledOnce()
  })

  it('cancelled before the first tick fires nothing', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    const off = repeat(clock, { delay: 10 }, fn)
    off()
    expect(clock.pending).toBe(0)
    clock.advance(100)
    expect(fn).not.toHaveBeenCalled()
  })

  it('the teardown is idempotent and harmless after the schedule retired', () => {
    const clock = fakeClock()
    const fn = vi.fn()
    const off = repeat(clock, { delay: 0, period: 5, count: 2 }, fn)
    clock.advance(5)
    expect(fn).toHaveBeenCalledTimes(2)
    expect(() => {
      off()
      off()
    }).not.toThrow()
    expect(clock.pending).toBe(0)
  })
})

describe('queue()', () => {
  it('buffers pushes before a pull, FIFO', async () => {
    const q = queue<number>()
    q.push(1)
    q.push(2)
    await expect(q.iterator.next()).resolves.toEqual({ value: 1, done: false })
    await expect(q.iterator.next()).resolves.toEqual({ value: 2, done: false })
  })

  it('a pull before a push waits and is resolved by the push', async () => {
    const q = queue<number>()
    const p = q.iterator.next()
    q.push(7)
    await expect(p).resolves.toEqual({ value: 7, done: false })
  })

  it('queues concurrent pulls and answers them in order', async () => {
    const q = queue<number>()
    const p1 = q.iterator.next()
    const p2 = q.iterator.next()
    q.push(1)
    q.push(2)
    await expect(p1).resolves.toEqual({ value: 1, done: false })
    await expect(p2).resolves.toEqual({ value: 2, done: false })
  })

  it('end() drains buffered values first, then reports done for good', async () => {
    const q = queue<number>()
    q.push(1)
    q.push(2)
    q.end()
    await expect(q.iterator.next()).resolves.toEqual({ value: 1, done: false })
    await expect(q.iterator.next()).resolves.toEqual({ value: 2, done: false })
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('end() resolves a waiting pull with done', async () => {
    const q = queue<number>()
    const p = q.iterator.next()
    q.end()
    await expect(p).resolves.toEqual({ value: undefined, done: true })
  })

  it('fail(e) drains buffered values, rejects exactly once, then reports done', async () => {
    const err = new Error('boom')
    const q = queue<number>()
    q.push(1)
    q.fail(err)
    await expect(q.iterator.next()).resolves.toEqual({ value: 1, done: false })
    await expect(q.iterator.next()).rejects.toBe(err)
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('fail(e) rejects a waiting pull', async () => {
    const err = new Error('boom')
    const q = queue<number>()
    const p = q.iterator.next()
    q.fail(err)
    await expect(p).rejects.toBe(err)
  })

  it('return() drops the buffer, fires onReturn once and reports done', async () => {
    const onReturn = vi.fn()
    const q = queue<number>({ onReturn })
    q.push(1)
    q.push(2)
    await expect(q.iterator.return()).resolves.toEqual({ value: undefined, done: true })
    expect(onReturn).toHaveBeenCalledOnce()
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
    await q.iterator.return()
    expect(onReturn).toHaveBeenCalledOnce()
  })

  it('return() resolves a waiting pull with done', async () => {
    const q = queue<number>()
    const p = q.iterator.next()
    await q.iterator.return()
    await expect(p).resolves.toEqual({ value: undefined, done: true })
  })

  it('throw(e) rejects the pending pull with e, rejects itself, and fires onReturn', async () => {
    const onReturn = vi.fn()
    const err = new Error('boom')
    const q = queue<number>({ onReturn })
    const p = q.iterator.next()
    const [pull, thrown] = await Promise.allSettled([p, q.iterator.throw(err)])
    expect(pull).toEqual({ status: 'rejected', reason: err })
    expect(thrown).toEqual({ status: 'rejected', reason: err })
    expect(onReturn).toHaveBeenCalledOnce()
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('throw(e) with nothing pending rejects the next pull once', async () => {
    const err = new Error('boom')
    const q = queue<number>()
    q.push(1)
    await expect(q.iterator.throw(err)).rejects.toBe(err)
    await expect(q.iterator.next()).rejects.toBe(err)
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('onReturn does not fire for a producer-side end() or fail()', async () => {
    const onReturn = vi.fn()
    const ended = queue<number>({ onReturn })
    ended.end()
    await ended.iterator.return()
    const failed = queue<number>({ onReturn })
    failed.fail(new Error('boom'))
    await expect(failed.iterator.next()).rejects.toThrow('boom')
    await failed.iterator.return()
    expect(onReturn).not.toHaveBeenCalled()
  })

  it('buffer caps held values, dropping the oldest', async () => {
    const q = queue<number>({ buffer: 2 })
    q.push(1)
    q.push(2)
    q.push(3)
    await expect(q.iterator.next()).resolves.toEqual({ value: 2, done: false })
    await expect(q.iterator.next()).resolves.toEqual({ value: 3, done: false })
  })

  it('ignores push after end, fail or return', async () => {
    const ended = queue<number>()
    ended.end()
    ended.push(1)
    await expect(ended.iterator.next()).resolves.toEqual({ value: undefined, done: true })

    const returned = queue<number>()
    await returned.iterator.return()
    returned.push(1)
    await expect(returned.iterator.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('a second terminal is a no-op', async () => {
    const q = queue<number>()
    q.end()
    q.fail(new Error('too late'))
    await expect(q.iterator.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('works with for await', async () => {
    const q = queue<number>()
    q.push(1)
    q.push(2)
    q.push(3)
    q.end()
    const out: number[] = []
    for await (const v of q.iterator) out.push(v)
    expect(out).toEqual([1, 2, 3])
    expect(q.iterator[Symbol.asyncIterator]()).toBe(q.iterator)
  })

  it('breaking out of for await counts as abandoning: onReturn fires', async () => {
    const onReturn = vi.fn()
    const q = queue<number>({ onReturn })
    q.push(1)
    q.push(2)
    const out: number[] = []
    for await (const v of q.iterator) {
      out.push(v)
      break
    }
    expect(out).toEqual([1])
    expect(onReturn).toHaveBeenCalledOnce()
  })
})

describe('emitter()', () => {
  it('delivers the emitted arguments to every listener', () => {
    const em = emitter<[string, number]>()
    const a = vi.fn()
    const b = vi.fn()
    em.add(a)
    em.add(b)
    em.emit('x', 1)
    expect(a).toHaveBeenCalledExactlyOnceWith('x', 1)
    expect(b).toHaveBeenCalledExactlyOnceWith('x', 1)
    expect(em.size).toBe(2)
  })

  it('one throwing listener does not starve the rest and is reported via onError', () => {
    const onError = vi.fn()
    const em = emitter<[number]>(onError)
    const boom = new Error('boom')
    const after = vi.fn()
    em.add(() => {
      throw boom
    })
    em.add(after)
    em.emit(1)
    expect(after).toHaveBeenCalledExactlyOnceWith(1)
    expect(onError).toHaveBeenCalledExactlyOnceWith(boom)
  })

  it('a signal detaches a listener, and an already-aborted signal never attaches', () => {
    const em = emitter<[number]>()
    const fn = vi.fn()
    const ac = new AbortController()
    em.add(fn, ac.signal)
    expect(em.size).toBe(1)
    ac.abort()
    expect(em.size).toBe(0)
    em.emit(1)
    expect(fn).not.toHaveBeenCalled()
    em.add(fn, ac.signal)
    expect(em.size).toBe(0)
  })

  it('the teardown is idempotent and safe alongside a signal', () => {
    const em = emitter<[]>()
    const fn = vi.fn()
    const ac = new AbortController()
    const off = em.add(fn, ac.signal)
    off()
    off()
    expect(em.size).toBe(0)
    expect(() => ac.abort()).not.toThrow()
    em.emit()
    expect(fn).not.toHaveBeenCalled()
  })

  it('a listener may detach from inside its own callback without skipping others', () => {
    const em = emitter<[]>()
    const after = vi.fn()
    let off: Unsub = () => {}
    off = em.add(() => off())
    em.add(after)
    em.emit()
    expect(after).toHaveBeenCalledOnce()
    expect(em.size).toBe(1)
    em.emit()
    expect(after).toHaveBeenCalledTimes(2)
  })

  it('clear() drops every listener', () => {
    const em = emitter<[]>()
    const fn = vi.fn()
    em.add(fn)
    em.add(vi.fn())
    em.clear()
    expect(em.size).toBe(0)
    em.emit()
    expect(fn).not.toHaveBeenCalled()
  })
})
