import {
  both,
  mount,
  pair,
  recv,
  send,
  symmetric,
  type Frame,
  type Meta,
  type Source,
  type Subscription,
  type Validate,
} from './index.js'
import { Hub, pair as linkPair, type Link, type ListenOptions, type Pair, type Port } from '../index.js'
import { describe, expect, it, vi } from 'vitest'

// --- harness ----------------------------------------------------------------

/** Counts live `listen` calls on a port — the single-listener invariant made visible. */
const probe = <S, R>(port: Port<S, R>) => {
  let count = 0
  return {
    get count() {
      return count
    },
    send: (v: S) => port.send(v),
    listen: (next: (v: R) => void, opts: ListenOptions = {}) => {
      count++
      let done = false
      const leave = () => {
        if (done) return
        done = true
        count--
      }
      const off = port.listen(next, {
        ...opts,
        onClose: (e) => {
          leave()
          opts.onClose?.(e)
        },
      })
      return () => {
        leave()
        off()
      }
    },
  }
}

/** An in-memory duplex: structured clone, microtask delivery, synchronous close. */
const wire = (opts?: Pair.Options) => {
  const [a, b] = linkPair<Frame>('left', 'right', opts)
  return { a: probe(a), b: probe(b), rawA: a, rawB: b }
}

/** Every frame one raw end sees, decoded by nobody. */
const spy = (link: Link<Frame>) => {
  const seen: Frame[] = []
  link.listen((f) => seen.push(f))
  return seen
}

/** Drains the microtask chains `pair` delivers on. */
const settled = () => new Promise<void>((r) => setTimeout(r, 0))

const gate = <T = void>() => {
  let resolve!: (v: T) => void
  const promise = new Promise<T>((r) => (resolve = r))
  return { promise, resolve }
}

/** A hand-driven port, for close-with-error and already-closed cases. */
const manual = <T>() => {
  type L = { next: (v: T) => void; onClose: ((e?: unknown) => void) | undefined }
  const ls = new Set<L>()
  const sent: T[] = []
  let closed: { error?: unknown } | null = null
  return {
    sent,
    get listeners() {
      return ls.size
    },
    send: (v: T) => {
      sent.push(v)
    },
    listen: (next: (v: T) => void, opts: ListenOptions = {}) => {
      if (closed) {
        opts.onClose?.(closed.error)
        return () => {}
      }
      const l: L = { next, onClose: opts.onClose }
      ls.add(l)
      return () => {
        ls.delete(l)
      }
    },
    deliver: (v: T) => {
      for (const l of [...ls]) l.next(v)
    },
    close: (error?: unknown) => {
      if (closed) return
      closed = { error }
      const all = [...ls]
      ls.clear()
      for (const l of all) l.onClose?.(error)
    },
  }
}

// --- fixture ----------------------------------------------------------------

interface User {
  id: string
  name: string
}

const stringSchema: Validate.StandardSchemaV1<string, string> = {
  '~standard': {
    version: 1,
    vendor: 'test',
    validate: (v) => (typeof v === 'string' ? { value: v } : { issues: [{ message: 'expected a string' }] }),
  },
}

const api = pair('api', {
  log: send(stringSchema),
  push: recv<{ at: number }>(),
  ready: both<void>(),
  getUser: send<{ id: string }>().reply<User>(),
  boom: send<void>().reply<string>(),
  watch: send<{ q: string }>().stream<number>(),
  ping: recv<void>().reply<number>(),
  echo: send<Record<string, unknown>>().reply<Record<string, unknown>>(),
})

// ----------------------------------------------------------------------------

describe('notifications', () => {
  it('delivers left → right, right → left, and both<T>() to the other side', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const heard: string[] = []
    const events: number[] = []
    const readyOnLeft: number[] = []
    const u1 = r.on.log((s) => heard.push(s))
    const u2 = l.on.push((e) => events.push(e.at))
    const u3 = l.on.ready(() => readyOnLeft.push(1))

    l.log('hello')
    l.log('again')
    r.push({ at: 7 })
    r.ready()
    await settled()

    expect(heard).toEqual(['hello', 'again'])
    expect(events).toEqual([7])
    expect(readyOnLeft).toEqual([1])
    u1()
    u2()
    u3()
    expect(t.a.count + t.b.count).toBe(0)
  })

  it('a side that only sends holds no transport listener', async () => {
    const t = wire()
    const l = api.left(t.a)
    l.log('x')
    await settled()
    expect(t.a.count).toBe(0)
  })

  it('on.X shares one listener per side and releases it when the last leaves', () => {
    const t = wire()
    const r = api.right(t.b)
    const u1 = r.on.log(() => {})
    const u2 = r.on.ready(() => {})
    expect(t.b.count).toBe(1)
    u1()
    expect(t.b.count).toBe(1)
    u2()
    expect(t.b.count).toBe(0)
  })

  it('a listener signal detaches', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const fn = vi.fn()
    const ac = new AbortController()
    r.on.log(fn, { signal: ac.signal })
    ac.abort()
    expect(t.b.count).toBe(0)
    l.log('x')
    await settled()
    expect(fn).not.toHaveBeenCalled()
  })
})

describe('request / response', () => {
  it('resolves with the response and keeps concurrent calls correlated', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const stop = r.serve({ getUser: async ({ id }) => ({ id, name: `user-${id}` }) })

    expect(await l.getUser({ id: '42' })).toEqual({ id: '42', name: 'user-42' })
    const [a, b] = await Promise.all([l.getUser({ id: '1' }), l.getUser({ id: '2' })])
    expect([a.name, b.name]).toEqual(['user-1', 'user-2'])
    stop()
  })

  it('rejects with a rehydrated error when the handler throws', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({
      boom: () => {
        throw new TypeError('handler exploded')
      },
    })
    const err = await l.boom().catch((e: unknown) => e)
    expect(err).toBeInstanceOf(Error)
    expect((err as Error).message).toBe('handler exploded')
    expect((err as Error).name).toBe('TypeError')
  })

  it('serve holds exactly one listener and releases it', () => {
    const t = wire()
    const r = api.right(t.b)
    expect(t.b.count).toBe(0)
    const stop = r.serve({ getUser: async ({ id }) => ({ id, name: 'x' }), boom: () => 'ok' })
    expect(t.b.count).toBe(1)
    stop()
    expect(t.b.count).toBe(0)
  })

  it('attaches for the duration of a call and releases once settled', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({ getUser: async ({ id }) => ({ id, name: 'x' }) })
    expect(t.a.count).toBe(0)
    const p = l.getUser({ id: '1' })
    expect(t.a.count).toBe(1)
    await p
    expect(t.a.count).toBe(0)
  })

  it('a call aborted via signal sends stop, rejects with the reason, and aborts the handler', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const g = gate()
    let aborted = false
    r.serve({
      getUser: async ({ id }, ctx) => {
        ctx.signal.addEventListener('abort', () => (aborted = true))
        await g.promise
        return { id, name: 'late' }
      },
    })
    const toRight = spy(t.rawB)
    const toLeft = spy(t.rawA)
    const ac = new AbortController()
    const reason = new Error('nevermind')

    const p = l.getUser({ id: '1' }, { signal: ac.signal })
    await settled()
    ac.abort(reason)
    await expect(p).rejects.toBe(reason)
    await settled()

    expect(aborted).toBe(true)
    expect(toRight).toContainEqual(expect.objectContaining({ kind: 'getUser', re: 'stop' }))
    g.resolve()
    await settled()
    expect(toLeft.filter((f) => f.re === 'ok')).toEqual([])
    expect(t.a.count).toBe(0)
  })

  it('an already-aborted signal rejects without sending or attaching', async () => {
    const t = wire()
    const l = api.left(t.a)
    const seen = spy(t.rawB)
    const ac = new AbortController()
    const reason = new Error('never started')
    ac.abort(reason)
    await expect(l.getUser({ id: '1' }, { signal: ac.signal })).rejects.toBe(reason)
    await settled()
    expect(seen).toEqual([])
    expect(t.a.count).toBe(0)
  })

  it('a handler that throws after its signal aborted sends nothing', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const g = gate()
    r.serve({
      getUser: async () => {
        await g.promise
        throw new Error('too late')
      },
    })
    const toLeft = spy(t.rawA)
    const ac = new AbortController()
    const p = l.getUser({ id: '1' }, { signal: ac.signal })
    await settled()
    ac.abort()
    await p.catch(() => {})
    g.resolve()
    await settled()
    expect(toLeft.filter((f) => f.re === 'err')).toEqual([])
  })

  it('reads a lone argument as options only when it carries an AbortSignal', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    let pingPayload: unknown = 'unset'
    r.serve({ echo: (p) => p })
    l.serve({
      ping: (p) => {
        pingPayload = p
        return 1
      },
    })
    expect(await l.echo({})).toEqual({})
    expect(await r.ping({ signal: new AbortController().signal })).toBe(1)
    expect(pingPayload).toBeUndefined()
  })
})

describe('streaming', () => {
  it('delivers every value then completes, releasing the listener', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const stop = r.serve({
      watch: async function* () {
        yield 0
        yield 1
        yield 2
      },
    })
    const got: number[] = []
    for await (const n of l.watch({ q: 'x' })) got.push(n)
    expect(got).toEqual([0, 1, 2])
    await settled()
    expect(t.a.count).toBe(0)
    stop()
    expect(t.b.count).toBe(0)
  })

  it('is lazy: nothing is sent until the first pull', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({
      watch: async function* () {
        yield 0
      },
    })
    const seen = spy(t.rawB)
    const it = l.watch({ q: 'x' })
    await settled()
    expect(seen).toEqual([])
    expect(t.a.count).toBe(0)
    expect(await it.next()).toEqual({ value: 0, done: false })
    expect(seen.map((f) => [f.kind, f.re])).toEqual([['watch', undefined]])
    await it.return?.()
  })

  it('queues values between pulls', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({ watch: () => [1, 2, 3] })
    const it = l.watch({ q: 'x' })
    expect(await it.next()).toEqual({ value: 1, done: false })
    await settled()
    await settled()
    expect(await it.next()).toEqual({ value: 2, done: false })
    expect(await it.next()).toEqual({ value: 3, done: false })
    expect(await it.next()).toEqual({ value: undefined, done: true })
  })

  it('break sends stop and runs the handler iterator to its finally', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    let finals = 0
    r.serve({
      watch: async function* () {
        try {
          yield 1
          yield 2
          yield 3
        } finally {
          finals++
        }
      },
    })
    const toRight = spy(t.rawB)
    for await (const n of l.watch({ q: 'y' })) {
      if (n === 1) break
    }
    await settled()
    await settled()
    expect(finals).toBe(1)
    expect(toRight).toContainEqual(expect.objectContaining({ kind: 'watch', re: 'stop' }))
    expect(t.a.count).toBe(0)
  })

  it('a stop that arrives while the handler awaits drops the late value', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const g = gate()
    let finals = 0
    r.serve({
      watch: async function* () {
        try {
          yield 1
          await g.promise
          yield 2
        } finally {
          finals++
        }
      },
    })
    const toLeft = spy(t.rawA)
    const it = l.watch({ q: 'z' })
    expect(await it.next()).toEqual({ value: 1, done: false })
    await it.return?.()
    await settled()
    g.resolve()
    await settled()
    await settled()
    expect(finals).toBe(1)
    expect(toLeft.filter((f) => f.re === 'next').map((f) => f.payload)).toEqual([1])
  })

  it('aborting the signal rejects the pending pull with the reason and sends stop', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const g = gate()
    r.serve({
      watch: async function* () {
        yield 1
        await g.promise
        yield 2
      },
    })
    const toRight = spy(t.rawB)
    const ac = new AbortController()
    const reason = new Error('enough')
    const it = l.watch({ q: 'a' }, { signal: ac.signal })
    expect(await it.next()).toEqual({ value: 1, done: false })
    const pull = it.next()
    ac.abort(reason)
    await expect(pull).rejects.toBe(reason)
    await settled()
    expect(toRight).toContainEqual(expect.objectContaining({ kind: 'watch', re: 'stop' }))
    g.resolve()
  })

  it('a throwing handler rejects the consumer after the values already sent', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({
      watch: async function* () {
        yield 1
        yield 2
        throw new RangeError('kaboom')
      },
    })
    const got: number[] = []
    let caught: unknown
    try {
      for await (const n of l.watch({ q: 'x' })) got.push(n)
    } catch (e) {
      caught = e
    }
    expect(got).toEqual([1, 2])
    expect(caught).toBeInstanceOf(Error)
    expect((caught as Error).message).toBe('kaboom')
    expect((caught as Error).name).toBe('RangeError')
  })

  it('a source that completes at once ends the stream exactly once', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({ watch: () => [] })
    const toLeft = spy(t.rawA)
    let ran = false
    for await (const _ of l.watch({ q: 'x' })) ran = true
    await settled()
    expect(ran).toBe(false)
    expect(toLeft.filter((f) => f.re === 'end')).toHaveLength(1)
  })

  it('a handler that throws after the requester stopped sends no err frame', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const g = gate()
    r.serve({
      watch: async function* () {
        yield 1
        await g.promise
        throw new Error('late')
      },
    })
    const toLeft = spy(t.rawA)
    const it = l.watch({ q: 'x' })
    await it.next()
    await it.return?.()
    await settled()
    g.resolve()
    await settled()
    expect(toLeft.filter((f) => f.re === 'err')).toEqual([])
  })

  it('return() before the first pull sends nothing and attaches nothing', async () => {
    const t = wire()
    const l = api.left(t.a)
    const seen = spy(t.rawB)
    const it = l.watch({ q: 'x' })
    await it.return?.()
    await settled()
    expect(seen).toEqual([])
    expect(t.a.count).toBe(0)
  })
})

describe('streaming from a push source', () => {
  /** A hand-driven push source, with its subscriber count visible. */
  const pushable = () => {
    type Sub = { next: (v: number) => void; close: (e?: unknown) => void }
    const subs = new Set<Sub>()
    return {
      get subscribers() {
        return subs.size
      },
      push: (v: number) => {
        for (const s of [...subs]) s.next(v)
      },
      close: (e?: unknown) => {
        for (const s of [...subs]) s.close(e)
      },
      source: (): Source<number> => (next, close) => {
        const sub = { next, close }
        subs.add(sub)
        return () => subs.delete(sub)
      },
    }
  }

  it('forwards pushed values and completes on close()', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const src = pushable()
    r.serve({ watch: () => src.source() })

    const got: number[] = []
    const done = (async () => {
      for await (const n of l.watch({ q: 'x' })) got.push(n)
    })()
    await settled()
    expect(src.subscribers).toBe(1)

    src.push(1)
    src.push(2)
    src.close()
    await done

    expect(got).toEqual([1, 2])
    expect(src.subscribers).toBe(0)
  })

  it('fails the stream when the source closes with an error', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const src = pushable()
    r.serve({ watch: () => src.source() })

    const it = l.watch({ q: 'x' })
    const first = it.next()
    await settled()
    src.close(new Error('upstream gone'))

    await expect(first).rejects.toThrow('upstream gone')
    expect(src.subscribers).toBe(0)
  })

  it('unsubscribes when the requester stops', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const src = pushable()
    r.serve({ watch: () => src.source() })

    const it = l.watch({ q: 'x' })
    void it.next()
    await settled()
    expect(src.subscribers).toBe(1)

    await it.return?.()
    await settled()
    expect(src.subscribers).toBe(0)
    // Anything pushed afterwards is dropped rather than sent.
    const toLeft = spy(t.rawA)
    src.push(9)
    await settled()
    expect(toLeft).toEqual([])
  })

  it('unsubscribes when the transport closes under it', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const src = pushable()
    r.serve({ watch: () => src.source() })
    const it = l.watch({ q: 'x' })
    void it.next()
    await settled()

    t.rawA.close()
    await settled()
    expect(src.subscribers).toBe(0)
  })

  it('handles a source that pushes and closes synchronously, before its teardown exists', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    let torn = 0
    r.serve({
      watch: () => (next, close) => {
        next(1)
        next(2)
        close()
        return () => torn++
      },
    })

    const got: number[] = []
    for await (const n of l.watch({ q: 'x' })) got.push(n)
    expect(got).toEqual([1, 2])
    expect(torn).toBe(1)
  })

  it('swallows a teardown that throws rather than losing it into the abort', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({
      watch: () => (next) => {
        next(1)
        return () => {
          throw new Error('bad teardown')
        }
      },
    })
    const it = l.watch({ q: 'x' })
    expect(await it.next()).toEqual({ value: 1, done: false })
    await expect(it.return?.()).resolves.toEqual({ value: undefined, done: true })
    await settled()
  })

  it('fails the stream when subscribing throws', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.serve({
      watch: () => () => {
        throw new Error('cannot subscribe')
      },
    })
    await expect(l.watch({ q: 'x' }).next()).rejects.toThrow('cannot subscribe')
  })

  it('takes a source from an async handler, and a port listener as one', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const [feed, upstream] = linkPair<number>()
    r.serve({ watch: async () => (next, close) => upstream.listen(next, { onClose: close }) })

    const it = l.watch({ q: 'x' })
    const first = it.next()
    await settled()
    feed.send(7)
    expect(await first).toEqual({ value: 7, done: false })

    feed.close()
    expect(await it.next()).toEqual({ value: undefined, done: true })
  })
})

describe('on.X raw API', () => {
  it('exposes payload, respond, fail and signal for calls; a second respond is ignored', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const toLeft = spy(t.rawA)
    r.on.getUser(({ payload, respond, signal }) => {
      expect(signal).toBeInstanceOf(AbortSignal)
      respond({ id: payload.id, name: 'raw' })
      respond({ id: 'x', name: 'again' })
    })
    expect(await l.getUser({ id: '9' })).toEqual({ id: '9', name: 'raw' })
    await settled()
    expect(toLeft.filter((f) => f.re === 'ok')).toHaveLength(1)
  })

  it('exposes next, end, fail and signal for streams; signal aborts on stop, next after end is dropped', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    let inc: Subscription<{ q: string }, number> | undefined
    const u = r.on.watch((s) => {
      inc = s
      s.next(1)
    })
    const it = l.watch({ q: 'x' })
    expect(await it.next()).toEqual({ value: 1, done: false })
    expect(inc?.signal.aborted).toBe(false)
    await it.return?.()
    await settled()
    expect(inc?.signal.aborted).toBe(true)
    u()

    const toLeft = spy(t.rawA)
    r.on.watch((s) => {
      s.next(2)
      s.end()
      s.next(3)
    })
    const got: number[] = []
    for await (const n of l.watch({ q: 'y' })) got.push(n)
    expect(got).toEqual([2])
    await settled()
    expect(toLeft.filter((f) => f.re === 'next').map((f) => f.payload)).toEqual([2])
  })
})

describe('meta', () => {
  it('rides along with a notification and reaches the listener', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const toRight = spy(t.rawB)
    const seen: [string, Meta][] = []
    r.on.log((s, meta) => seen.push([s, meta]))

    l.log('hello', { meta: { trace: 't1' } })
    await settled()

    expect(seen).toEqual([['hello', { trace: 't1' }]])
    expect(toRight[0]).toMatchObject({ kind: 'log', payload: 'hello', meta: { trace: 't1' } })
  })

  it('leaves the frame untouched when nothing is attached, and hands the listener an empty bag', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const toRight = spy(t.rawB)
    const seen: Meta[] = []
    r.on.log((_s, meta) => seen.push(meta))

    l.log('hello')
    await settled()

    expect(seen).toEqual([{}])
    expect('meta' in toRight[0]!).toBe(false)
  })

  it('is the lone argument on a message that takes no payload', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const seen: Meta[] = []
    r.on.ready((_p, meta) => seen.push(meta))

    l.ready({ meta: { n: 1 } })
    l.ready()
    await settled()

    expect(seen).toEqual([{ n: 1 }, {}])
  })

  it('reaches every kind of served handler through ctx', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const notes: Meta[] = []
    let asked: Meta | undefined
    let watched: Meta | undefined
    r.serve({
      log: (_s, ctx) => notes.push(ctx.meta),
      getUser: ({ id }, ctx) => {
        asked = ctx.meta
        return { id, name: 'x' }
      },
      watch: (_q, ctx) => {
        watched = ctx.meta
        return [1, 2]
      },
    })

    l.log('x', { meta: { a: 1 } })
    expect(await l.getUser({ id: '1' }, { meta: { b: 2 } })).toEqual({ id: '1', name: 'x' })
    const got: number[] = []
    for await (const n of l.watch({ q: 'y' }, { meta: { c: 3 } })) got.push(n)
    await settled()

    expect(notes).toEqual([{ a: 1 }])
    expect(asked).toEqual({ b: 2 })
    expect(watched).toEqual({ c: 3 })
    expect(got).toEqual([1, 2])
  })

  it('travels back on a response, where a raw listener reads it', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    const replies: (Meta | undefined)[] = []
    const off = l.listen((m) => {
      if (m.re !== undefined && m.re !== 'stop') replies.push(m.meta)
    })
    r.on.getUser(({ payload, respond }) => respond({ id: payload.id, name: 'x' }, { ms: 3 }))
    r.on.watch(({ next, end }) => {
      next(1, { i: 0 })
      next(2, { i: 1 })
      end({ total: 2 })
    })
    r.on.boom(({ fail }) => fail(new Error('nope'), { retry: false }))

    expect(await l.getUser({ id: '1' })).toEqual({ id: '1', name: 'x' })
    const got: number[] = []
    for await (const n of l.watch({ q: 'y' })) got.push(n)
    await expect(l.boom()).rejects.toThrow('nope')
    await settled()
    off()

    expect(got).toEqual([1, 2])
    expect(replies).toEqual([{ ms: 3 }, { i: 0 }, { i: 1 }, { total: 2 }, { retry: false }])
  })

  it('rides through a `signal` alongside it, and through an outer codec', async () => {
    const t = wire()
    const json = api.with<Frame>({
      encode: (m) => JSON.parse(JSON.stringify(m)) as Frame,
      decode: (f, next) => next(f),
    })
    const l = json.left(t.a)
    const r = json.right(t.b)
    let asked: Meta | undefined
    r.serve({ getUser: ({ id }, ctx) => ((asked = ctx.meta), { id, name: 'x' }) })

    const ac = new AbortController()
    await l.getUser({ id: '1' }, { meta: { trace: 't' }, signal: ac.signal })

    expect(asked).toEqual({ trace: 't' })
  })

  it('a codec contributes meta of its own through `next`', async () => {
    type Env = { hop: string; frame: Frame }
    const enveloped = api.with<Env>({
      encode: (frame) => ({ hop: 'edge', frame }),
      decode: (env, next) => next(env.frame, { hop: env.hop }),
    })
    const [pa, pb] = linkPair<Env>()
    const l = enveloped.left(pa)
    const r = enveloped.right(pb)

    const seen: Meta[] = []
    r.on.log((_s, meta) => seen.push(meta))
    let asked: Meta | undefined
    r.serve({
      getUser: ({ id }, ctx) => {
        asked = ctx.meta
        return { id, name: 'x' }
      },
    })

    l.log('hi', { meta: { trace: 't' } })
    expect(await l.getUser({ id: '1' })).toEqual({ id: '1', name: 'x' })
    await settled()

    // What the sender attached and what the layer knew, in one bag.
    expect(seen).toEqual([{ trace: 't', hop: 'edge' }])
    expect(asked).toEqual({ hop: 'edge' })
    pa.close()
  })

  it('composed codecs merge what each contributes, the outermost winning', async () => {
    type Mid = { v: 1; frame: Frame }
    type Out = { hop: string; body: Mid }
    const layered = api
      .with<Mid>({
        encode: (frame) => ({ v: 1, frame }),
        decode: (m, next) => next(m.frame, { hop: 'inner', layer: 'inner' }),
      })
      .with<Out>({
        encode: (mid) => ({ hop: 'edge', body: mid }),
        decode: (o, next) => next(o.body, { hop: o.hop }),
      })
    const [pa, pb] = linkPair<Out>()
    const l = layered.left(pa)
    const r = layered.right(pb)

    const seen: Meta[] = []
    r.on.log((_s, meta) => seen.push(meta))

    // The far side claims a `hop` of its own; both layers outrank it.
    l.log('hi', { meta: { hop: 'spoofed', trace: 't' } })
    await settled()

    expect(seen).toEqual([{ trace: 't', layer: 'inner', hop: 'edge' }])
    pa.close()
  })

  it('a payload shaped like options is disambiguated by passing the options', async () => {
    const t = wire()
    const l = api.left(t.a)
    const r = api.right(t.b)
    r.on.echo(({ payload, respond }) => respond(payload))

    // Bare, the lone `{ meta }` object is read as options — the documented cost
    // of an erased payload type.
    expect(await l.echo({ meta: { a: 1 } })).toBeUndefined()
    // Spelled out, it is the payload.
    expect(await l.echo({ meta: { a: 1 } }, {})).toEqual({ meta: { a: 1 } })
  })

  it('rejects a frame whose meta is not a bag of keys', async () => {
    const errors: unknown[] = []
    const t = wire({ onError: (e) => errors.push(e) })
    const strict = pair('v', { log: send(stringSchema) })
    const r = strict.right(t.b)
    const seen: unknown[] = []
    r.on.log((x) => seen.push(x))

    t.rawA.send({ _t: 'v', kind: 'log', payload: 'x', meta: 7 } as unknown as Frame)
    t.rawA.send({ _t: 'v', kind: 'log', payload: 'fine' })
    await settled()

    expect(errors.map((e) => (e as Error).message)).toEqual(['[v] log: meta must be an object'])
    expect(seen).toEqual(['fine'])
  })
})

describe('validation', () => {
  it('rejects a bad payload at the decode boundary; the throw lands in the link onError', async () => {
    const errors: unknown[] = []
    const t = wire({ onError: (e) => errors.push(e) })
    const strict = pair('v', { log: send(stringSchema) })
    const r = strict.right(t.b)
    const seen: unknown[] = []
    r.on.log((x) => seen.push(x))
    t.rawA.send({ _t: 'v', kind: 'log', payload: 99 })
    await settled()
    expect(errors).toHaveLength(1)
    expect((errors[0] as Error).message).toBe('[v] bad log payload: expected a string')
    expect(seen).toEqual([])
  })

  it('the onInvalid callback reports the reason and the stream carries on', async () => {
    const reasons: string[] = []
    const noticed = pair('v', { log: send(stringSchema) }, { onInvalid: (r) => reasons.push(r) })
    const t = wire()
    const r = noticed.right(t.b)
    const seen: unknown[] = []
    r.on.log((x) => seen.push(x))
    t.rawA.send({ _t: 'v', kind: 'log', payload: 99 })
    t.rawA.send({ _t: 'v', kind: 'log', payload: 'fine' })
    await settled()
    expect(reasons).toEqual(['bad log payload: expected a string'])
    expect(seen).toEqual(['fine'])
  })

  it("onInvalid: 'drop' skips silently", async () => {
    const lax = pair('v', { log: send(stringSchema) }, { onInvalid: 'drop' })
    const t = wire()
    const r = lax.right(t.b)
    const seen: unknown[] = []
    r.on.log((x) => seen.push(x))
    t.rawA.send({ _t: 'v', kind: 'log', payload: 99 })
    t.rawA.send({ _t: 'v', kind: 'log', payload: 'fine' })
    await settled()
    expect(seen).toEqual(['fine'])
  })

  it('ignores frames belonging to another protocol', async () => {
    const errors: unknown[] = []
    const t = wire({ onError: (e) => errors.push(e) })
    const strict = pair('v', { log: send(stringSchema) })
    const r = strict.right(t.b)
    const seen: unknown[] = []
    r.on.log((x) => seen.push(x))
    t.rawA.send({ _t: 'other', kind: 'log', payload: 99 })
    await settled()
    expect(errors).toEqual([])
    expect(seen).toEqual([])
  })
})

describe('direction enforcement', () => {
  it('rejects a message sent against its declared direction, and an unknown kind', async () => {
    const errors: unknown[] = []
    const t = wire({ onError: (e) => errors.push(e) })
    const strict = pair('dir', { log: send<string>(), push: recv<{ at: number }>() })
    const l = strict.left(t.a)
    l.on.push(() => {})
    // `log` is left-originated; the left side should never receive one
    t.rawB.send({ _t: 'dir', kind: 'log', payload: 'spoof' })
    t.rawB.send({ _t: 'dir', kind: 'nonsense', payload: 1 })
    await settled()
    expect(errors.map((e) => (e as Error).message)).toEqual([
      '[dir] peer may not send log',
      '[dir] unknown message: nonsense',
    ])
  })

  it('refuses reserved message names at runtime too', () => {
    expect(() => pair('bad', { then: send<void>() } as never)).toThrow('reserved')
  })
})

describe('symmetric', () => {
  const room = symmetric('room', {
    chat: both<string>(),
    ping: both<void>().reply<number>(),
    history: both<{ q: string }>().stream<number>(),
  })

  it('gives every instance the same surface, in both directions', async () => {
    const t = wire()
    const a = room.connect(t.a)
    const b = room.connect(t.b)
    const heardByA: string[] = []
    const heardByB: string[] = []

    a.on.chat((s) => heardByA.push(s))
    b.on.chat((s) => heardByB.push(s))
    a.chat('from a')
    b.chat('from b')
    await settled()

    expect(heardByA).toEqual(['from b'])
    expect(heardByB).toEqual(['from a'])
  })

  it('serves calls and streams from either end', async () => {
    const t = wire()
    const a = room.connect(t.a)
    const b = room.connect(t.b)
    a.serve({ ping: () => 1, history: ({ q }) => [q.length, q.length + 1] })
    b.serve({ ping: () => 2 })

    expect(await a.ping()).toBe(2)
    expect(await b.ping()).toBe(1)

    const rows: number[] = []
    for await (const n of b.history({ q: 'abc' })) rows.push(n)
    expect(rows).toEqual([3, 4])
  })

  it('keeps concurrent calls apart when both ends pick the same correlation id', async () => {
    const t = wire()
    const a = room.connect(t.a)
    const b = room.connect(t.b)
    const slow = gate<number>()
    a.serve({ ping: () => slow.promise })
    b.serve({ ping: () => 2 })

    // Both are the first call from their end, so both are id '1'.
    const fromA = a.ping()
    const fromB = b.ping()
    slow.resolve(1)

    expect(await fromA).toBe(2)
    expect(await fromB).toBe(1)
  })

  it('cancels the right request when both ends have one open under the same id', async () => {
    const t = wire()
    const a = room.connect(t.a)
    const b = room.connect(t.b)
    const aborted: string[] = []
    a.on.history(({ signal }) => signal.addEventListener('abort', () => aborted.push('a')))
    b.on.history(({ signal }) => signal.addEventListener('abort', () => aborted.push('b')))

    const fromA = a.history({ q: 'x' })
    const fromB = b.history({ q: 'y' })
    void fromA.next()
    void fromB.next()
    await settled()

    await fromA.return?.()
    await settled()
    // A stopped its own request, which B was answering.
    expect(aborted).toEqual(['b'])
  })

  it('renames and composes like a pair', async () => {
    const t = wire()
    const a = room.at('room.v2').connect(t.a)
    const seen = spy(t.rawB)
    a.chat('hi')
    await settled()
    expect(seen).toEqual([{ _t: 'room.v2', kind: 'chat', payload: 'hi' }])
  })

  it('refuses one-directional and reserved message names at runtime', () => {
    expect(() => symmetric('bad', { log: send<string>() } as never)).toThrow('one-directional')
    expect(() => symmetric('bad', { then: both<void>() } as never)).toThrow('reserved')
  })
})

describe('mount', () => {
  it('routes without cross-talk, .at() namespaces, and each protocol attaches only while listened to', async () => {
    const chat = pair('chat', { say: send<string>(), heard: recv<string>() })
    const app = mount({ api, chat, chat2: chat.at('chat.v2') })
    const t = wire()
    const l = app.left(t.a)
    const r = app.right(t.b)
    const said: string[] = []
    const said2: string[] = []
    const logged: string[] = []

    expect(t.b.count).toBe(0)
    const u1 = r.chat.on.say((s) => said.push(s))
    const u2 = r.chat2.on.say((s) => said2.push(s))
    const u3 = r.api.on.log((s) => logged.push(s))
    expect(t.b.count).toBe(3)
    const u4 = r.api.on.ready(() => {})
    expect(t.b.count).toBe(3)

    l.chat.say('one')
    l.chat2.say('two')
    l.api.log('three')
    await settled()

    expect(said).toEqual(['one'])
    expect(said2).toEqual(['two'])
    expect(logged).toEqual(['three'])
    u1()
    u2()
    u3()
    u4()
    expect(t.b.count).toBe(0)
  })

  it('mounts a channel alongside protocols, identical on both ends', async () => {
    const room = symmetric('room', { chat: both<string>() })
    const app = mount({ api, room })
    const t = wire()
    const l = app.left(t.a)
    const r = app.right(t.b)
    const said: string[] = []

    l.room.on.chat((s) => said.push(s))
    r.room.chat('one')
    l.room.chat('two')
    await settled()

    expect(said).toEqual(['one'])
  })
})

describe('transport close', () => {
  it('rejects pending calls when the port closes cleanly', async () => {
    const t = wire()
    const l = api.left(t.a)
    const p = l.getUser({ id: '1' })
    await settled()
    t.rawA.close()
    await expect(p).rejects.toThrow('getUser: channel closed before a response arrived')
  })

  it('rejects pending calls with the error when the port closes with one', async () => {
    const m = manual<Frame>()
    const l = api.left(m)
    const p = l.getUser({ id: '1' })
    m.close(new Error('boom'))
    await expect(p).rejects.toThrow('boom')
  })

  it('ends pending streams on a clean close and rejects them on an error close', async () => {
    const m = manual<Frame>()
    const l = api.left(m)
    const it = l.watch({ q: 'x' })
    const first = it.next()
    m.deliver({ _t: 'api', kind: 'watch', id: '1', re: 'next', payload: 5 })
    expect(await first).toEqual({ value: 5, done: false })
    m.close()
    expect(await it.next()).toEqual({ value: undefined, done: true })

    const m2 = manual<Frame>()
    const l2 = api.left(m2)
    const it2 = l2.watch({ q: 'x' })
    const pull = it2.next()
    m2.close(new Error('bad'))
    await expect(pull).rejects.toThrow('bad')
  })

  it('propagates onClose to on.X listeners exactly once, and reports closed to late ones', async () => {
    const m = manual<Frame>()
    const l = api.left(m)
    const onClose = vi.fn()
    l.on.push(() => {}, { onClose })
    m.close()
    m.close()
    expect(onClose).toHaveBeenCalledTimes(1)
    expect(onClose).toHaveBeenCalledWith(undefined)

    await expect(l.getUser({ id: '1' })).rejects.toThrow('channel closed')
    const late = vi.fn()
    l.on.push(() => {}, { onClose: late })
    expect(late).toHaveBeenCalledTimes(1)
  })

  it('aborts inbound handler signals when the port closes', () => {
    const m = manual<Frame>()
    const r = api.right(m)
    let sig: AbortSignal | undefined
    r.on.getUser(({ signal }) => {
      sig = signal
    })
    m.deliver({ _t: 'api', kind: 'getUser', id: '7', payload: { id: '1' } })
    expect(sig?.aborted).toBe(false)
    m.close()
    expect(sig?.aborted).toBe(true)
  })

  it('an already-closed port reports closure synchronously from listen', async () => {
    const m = manual<Frame>()
    m.close()
    const l = api.left(m)
    await expect(l.getUser({ id: '1' })).rejects.toThrow('channel closed')
    expect(m.listeners).toBe(0)
    expect(m.sent).toEqual([])
  })
})

describe('a Topic as the transport', () => {
  it('runs calls and streams over a hub and rejects pending calls when the session ends', async () => {
    type Bus = { api: Frame }
    const hub = Hub.create<Bus>('bus')
    const s1 = hub.local()
    const s2 = hub.local()
    await Promise.all([s1.ready(), s2.ready()])
    const l = api.left(s1.topic('api'))
    const r = api.right(s2.topic('api'))
    const stop = r.serve({
      getUser: async ({ id }) => ({ id, name: `user-${id}` }),
      watch: async function* () {
        yield 1
        yield 2
      },
    })

    await expect(l.getUser({ id: '1' })).resolves.toEqual({ id: '1', name: 'user-1' })
    const got: number[] = []
    for await (const n of l.watch({ q: 'x' })) got.push(n)
    expect(got).toEqual([1, 2])

    // lazy attach means the topic unsubscribed once nothing was pending
    await settled()
    expect(hub.peer(s1.id as string)?.channels.has('api')).toBe(false)

    stop()
    const p = l.getUser({ id: '2' })
    await settled()
    s1.close()
    await expect(p).rejects.toThrow('channel closed before a response arrived')
    hub.close()
  })
})
