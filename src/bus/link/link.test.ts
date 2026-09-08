import { describe, expect, it, vi } from 'vitest'
import { pair, persistent, type Link, type Persistent } from './index.ts'
import { fakeClock } from '../testing.js'

// --- harness ----------------------------------------------------------------

/** Drains the microtask chains `Link.pair` delivers on. */
const settled = () => new Promise<void>((r) => setTimeout(r, 0))

/** Everything a link delivers from now on (and anything it had buffered). */
const collect = <T>(link: Link<T>) => {
  const seen: T[] = []
  link.listen((m) => seen.push(m))
  return seen
}

// ----------------------------------------------------------------------------

describe('pair()', () => {
  it('delivers on a microtask and structured-clones the payload', async () => {
    const [a, b] = pair<{ n: number[] }>()
    const seen = collect(b)
    const msg = { n: [1] }
    expect(a.send(msg)).toBe(true)
    expect(seen).toEqual([])
    await settled()
    expect(seen).toEqual([msg])
    expect(seen[0]).not.toBe(msg)
    expect(seen[0]?.n).not.toBe(msg.n)
  })

  it('delivers in both directions, in order', async () => {
    const [a, b] = pair<number>()
    const seenA = collect(a)
    const seenB = collect(b)
    a.send(1)
    b.send(10)
    a.send(2)
    await settled()
    expect(seenB).toEqual([1, 2])
    expect(seenA).toEqual([10])
  })

  it('passes references with clone: false', async () => {
    const [a, b] = pair<{ n: number }>('A', 'B', { clone: false })
    const seen = collect(b)
    const msg = { n: 1 }
    a.send(msg)
    await settled()
    expect(seen[0]).toBe(msg)
  })

  it('returns false and reports when the payload cannot be cloned', async () => {
    const onError = vi.fn<(err: unknown) => void>()
    const [a, b] = pair<unknown>('A', 'B', { onError })
    const seen = collect(b)
    expect(a.send(() => 1)).toBe(false)
    expect(onError).toHaveBeenCalledOnce()
    expect(onError.mock.calls[0]?.[0]).toBeInstanceOf(Error)
    await settled()
    expect(seen).toEqual([])
    expect(a.up).toBe(true)
  })

  it('buffers inbound before the first listen and replays it once, in order', async () => {
    const [a, b] = pair<number>('A', 'B', { pending: 2 })
    a.send(1)
    a.send(2)
    await settled()
    const seen = collect(b)
    expect(seen).toEqual([1, 2])
    expect(collect(b)).toEqual([])
  })

  // SOURCE BUG (src/internal.ts, linkCore.deliver): the overflow loop is
  //   `while (pending.length >= cap) o.onDrop?.(pending.shift() as T)`
  // and `pair` passes no `onDrop`, so the optional call short-circuits without
  // evaluating its argument — `shift()` never runs and the loop never ends. Any
  // third message on a `pending: 2` pair hangs the process (65 on the default
  // 64). `fromPort` and the extension adapter pass no `onDrop` either. Skipped
  // rather than `it.fails` because the failure mode is an infinite loop.
  it('drops the oldest buffered inbound message past `pending`', async () => {
    const [a, b] = pair<number>('A', 'B', { pending: 2 })
    a.send(1)
    a.send(2)
    a.send(3)
    await settled()
    expect(collect(b)).toEqual([2, 3])
  })

  it('pending: 0 disables inbound buffering', async () => {
    const [a, b] = pair<number>('A', 'B', { pending: 0 })
    a.send(1)
    await settled()
    expect(collect(b)).toEqual([])
  })

  it('names the other end through remote and meta', () => {
    const [a, b] = pair('client', 'server', { metaA: { side: 'a' }, metaB: { side: 'b' } })
    expect(a.remote).toBe('server')
    expect(a.meta).toEqual({ side: 'b' })
    expect(b.remote).toBe('client')
    expect(b.meta).toEqual({ side: 'a' })
    const [x, y] = pair()
    expect(x.remote).toBe('B')
    expect(y.remote).toBe('A')
    expect(x.meta).toEqual({})
  })

  it('closing one end closes both synchronously', () => {
    const [a, b] = pair<number>()
    const closedA = vi.fn()
    const closedB = vi.fn()
    a.closed(closedA)
    b.closed(closedB)
    expect(a.up).toBe(true)
    expect(b.up).toBe(true)
    a.close()
    expect(a.up).toBe(false)
    expect(b.up).toBe(false)
    expect(closedA).toHaveBeenCalledOnce()
    expect(closedB).toHaveBeenCalledOnce()
    b.close()
    expect(closedA).toHaveBeenCalledOnce()
    expect(closedB).toHaveBeenCalledOnce()
  })

  it('closed fires immediately on an already-closed link', () => {
    const [a] = pair<number>()
    a.close()
    const fn = vi.fn()
    a.closed(fn)
    expect(fn).toHaveBeenCalledOnce()
  })

  it('listen onClose fires once, with no error, when the link closes', () => {
    const [a, b] = pair<number>()
    const onClose = vi.fn<(error?: unknown) => void>()
    const next = vi.fn()
    b.listen(next, { onClose })
    a.close()
    expect(onClose).toHaveBeenCalledOnce()
    expect(onClose.mock.calls[0]?.[0]).toBeUndefined()
    b.close()
    expect(onClose).toHaveBeenCalledOnce()
    expect(next).not.toHaveBeenCalled()
  })

  it('listen onClose fires synchronously on an already-closed link', () => {
    const [a, b] = pair<number>()
    a.close()
    const onClose = vi.fn()
    const off = b.listen(vi.fn(), { onClose })
    expect(onClose).toHaveBeenCalledOnce()
    expect(() => off()).not.toThrow()
  })

  it('listen onClose does not fire after unsubscribe', () => {
    const [a, b] = pair<number>()
    const onClose = vi.fn()
    const off = b.listen(vi.fn(), { onClose })
    off()
    a.close()
    expect(onClose).not.toHaveBeenCalled()
  })

  it('changed emits false on close', () => {
    const [a] = pair<number>()
    const fn = vi.fn<(up: boolean) => void>()
    a.changed(fn)
    a.close()
    expect(fn).toHaveBeenCalledExactlyOnceWith(false)
  })

  it('a signal detaches listen, changed and closed subscriptions', async () => {
    const [a, b] = pair<number>()
    const ac = new AbortController()
    const next = vi.fn()
    const changed = vi.fn()
    const closed = vi.fn()
    const onClose = vi.fn()
    b.listen(next, { signal: ac.signal, onClose })
    b.changed(changed, { signal: ac.signal })
    b.closed(closed, { signal: ac.signal })
    ac.abort()
    a.send(1)
    await settled()
    a.close()
    expect(next).not.toHaveBeenCalled()
    expect(changed).not.toHaveBeenCalled()
    expect(closed).not.toHaveBeenCalled()
    expect(onClose).not.toHaveBeenCalled()
  })

  it('send after close returns false and nothing in flight is delivered', async () => {
    const [a, b] = pair<number>()
    const seen = collect(b)
    expect(a.send(1)).toBe(true)
    a.close()
    expect(a.send(2)).toBe(false)
    expect(b.send(3)).toBe(false)
    await settled()
    expect(seen).toEqual([])
  })

  it('a throwing listener is reported and does not stop delivery to the rest', async () => {
    const onError = vi.fn()
    const [a, b] = pair<number>('A', 'B', { onError })
    const boom = new Error('boom')
    b.listen(() => {
      throw boom
    })
    const seen = collect(b)
    a.send(1)
    await settled()
    expect(seen).toEqual([1])
    expect(onError).toHaveBeenCalledExactlyOnceWith(boom)
  })
})

// ----------------------------------------------------------------------------

describe('persistent()', () => {
  /**
   * A persistent link over a connector that mints a fresh `pair` per attempt.
   * Every server end is kept so a test can greet it, kill it, or read what it got.
   */
  const rig = (opts: Persistent.Options = {}) => {
    const clock = fakeClock()
    const links: Link<string>[] = []
    const onState = vi.fn<(state: Persistent.State) => void>()
    const onError = vi.fn<(err: unknown) => void>()
    const onDrop = vi.fn<(msg: unknown) => void>()
    const connector = vi.fn((): Link<string> => {
      const n = links.length + 1
      const [client, server] = pair<string>('client', `server#${n}`, { metaB: { n } })
      links.push(server)
      return client
    })
    const link = persistent<string>(connector, { clock, jitter: 0, onState, onError, onDrop, ...opts })
    return {
      clock,
      links,
      link,
      connector,
      onState,
      onError,
      onDrop,
      /** The server end of the current attempt. */
      server: () => links.at(-1) as Link<string>,
      /** The server speaks first — that unprompted greeting is what flips the link open. */
      greet: async (msg = 'ready') => {
        links.at(-1)?.send(msg)
        await settled()
      },
      states: () => onState.mock.calls.map(([s]) => s),
      errors: () => onError.mock.calls.map(([e]) => (e instanceof Error ? e.message : String(e))),
      drops: () => onDrop.mock.calls.map(([m]) => m),
    }
  }

  it('starts connecting: one connector call, down, no failures yet', () => {
    const r = rig()
    expect(r.connector).toHaveBeenCalledOnce()
    expect(r.link.state).toBe('connecting')
    expect(r.link.up).toBe(false)
    expect(r.link.attempts).toBe(0)
    expect(r.states()).toEqual([])
    expect(r.clock.pending).toBe(1)
  })

  it('goes open on the first inbound message and reports onState', async () => {
    const r = rig()
    const changed = vi.fn<(up: boolean) => void>()
    r.link.changed(changed)
    const seen = collect(r.link)
    await r.greet('hello')
    expect(r.link.state).toBe('open')
    expect(r.link.up).toBe(true)
    expect(r.states()).toEqual(['open'])
    expect(changed).toHaveBeenCalledExactlyOnceWith(true)
    expect(seen).toEqual(['hello'])
    expect(r.clock.pending).toBe(0)
  })

  it('delivers inbound through listen, buffering anything before the first listener', async () => {
    const r = rig()
    await r.greet('one')
    r.server().send('two')
    await settled()
    expect(collect(r.link)).toEqual(['one', 'two'])
  })

  it('buffers sends while connecting and flushes them in order on open', async () => {
    const r = rig()
    const got = collect(r.server())
    expect(r.link.send('a')).toBe(true)
    expect(r.link.send('b')).toBe(true)
    expect(got).toEqual([])
    await r.greet()
    expect(got).toEqual(['a', 'b'])
    expect(r.link.send('c')).toBe(true)
    await settled()
    expect(got).toEqual(['a', 'b', 'c'])
    expect(r.drops()).toEqual([])
  })

  it('times out a silent attempt, reports it, and retries with exponential backoff capped at max', () => {
    const r = rig({ timeout: 100, backoff: { max: 1000 } })
    r.clock.advance(100)
    expect(r.errors()).toEqual(['no greeting from server#1 within 100ms'])
    expect(r.link.state).toBe('retrying')
    expect(r.link.attempts).toBe(1)
    expect(r.states()).toEqual(['retrying'])
    expect(r.server().up).toBe(false)

    // 250
    r.clock.advance(249)
    expect(r.links).toHaveLength(1)
    r.clock.advance(1)
    expect(r.links).toHaveLength(2)
    expect(r.link.state).toBe('connecting')
    r.clock.advance(100)
    expect(r.link.attempts).toBe(2)

    // 500
    r.clock.advance(499)
    expect(r.links).toHaveLength(2)
    r.clock.advance(1)
    expect(r.links).toHaveLength(3)
    r.clock.advance(100)
    expect(r.link.attempts).toBe(3)

    // 1000
    r.clock.advance(999)
    expect(r.links).toHaveLength(3)
    r.clock.advance(1)
    expect(r.links).toHaveLength(4)
    r.clock.advance(100)
    expect(r.link.attempts).toBe(4)

    // min(2000, max) = 1000
    r.clock.advance(999)
    expect(r.links).toHaveLength(4)
    r.clock.advance(1)
    expect(r.links).toHaveLength(5)
    expect(r.link.state).toBe('connecting')
    expect(r.errors()).toHaveLength(4)
    expect(r.errors().every((m) => /no greeting/.test(m))).toBe(true)
  })

  it('gives up after maxAttempts: reports, closes, and hands the outbox to onDrop', () => {
    const r = rig({ timeout: 100, maxAttempts: 2 })
    const closed = vi.fn()
    r.link.closed(closed)
    expect(r.link.send('queued')).toBe(true)
    r.clock.advance(100)
    expect(r.link.state).toBe('retrying')
    expect(closed).not.toHaveBeenCalled()
    r.clock.advance(250)
    expect(r.links).toHaveLength(2)
    r.clock.advance(100)
    expect(r.errors().at(-1)).toMatch(/giving up on server#2 after 2 attempts/)
    expect(r.link.state).toBe('closed')
    expect(r.link.up).toBe(false)
    expect(r.link.attempts).toBe(2)
    expect(r.states()).toEqual(['retrying', 'connecting', 'closed'])
    expect(closed).toHaveBeenCalledOnce()
    expect(r.drops()).toEqual(['queued'])
    expect(r.link.send('late')).toBe(false)
    expect(r.clock.pending).toBe(0)
    expect(r.connector).toHaveBeenCalledTimes(2)
  })

  it('reconnects when an open inner link dies and resets attempts on success', async () => {
    const r = rig({ timeout: 100 })
    const changed = vi.fn<(up: boolean) => void>()
    r.link.changed(changed)
    await r.greet()
    r.server().close()
    expect(r.link.state).toBe('retrying')
    expect(r.link.up).toBe(false)
    expect(r.link.attempts).toBe(1)
    expect(r.errors()).toEqual([])
    r.clock.advance(249)
    expect(r.links).toHaveLength(1)
    r.clock.advance(1)
    expect(r.links).toHaveLength(2)
    expect(r.link.state).toBe('connecting')
    const got = collect(r.server())
    await r.greet()
    expect(r.link.state).toBe('open')
    expect(r.link.attempts).toBe(0)
    expect(r.states()).toEqual(['open', 'retrying', 'connecting', 'open'])
    expect(changed.mock.calls.map(([up]) => up)).toEqual([true, false, true])
    r.link.send('after')
    await settled()
    expect(got).toEqual(['after'])
  })

  // SOURCE BUG (src/link.ts, persistent): the two outbox-shedding loops
  //   `while (outbox.length >= cap) opts.onDrop?.((outbox.shift() as Held<T>).msg)`   (overflow: 'oldest')
  //   `while (outbox.length > 0 && outbox[0].at < cutoff) opts.onDrop?.((outbox.shift() as Held<T>).msg)`   (expire)
  // short-circuit without evaluating `shift()` when the caller passes no `onDrop`,
  // so they never terminate. The rig above always passes `onDrop`, which is why
  // the ttl/overflow tests below are green; these two document the same
  // behaviour without it. Skipped rather than `it.fails` because they hang.
  it("sheds the oldest message with overflow: 'oldest' even without onDrop", async () => {
    const clock = fakeClock()
    const links: Link<string>[] = []
    const connector = (): Link<string> => {
      const [client, server] = pair<string>()
      links.push(server)
      return client
    }
    const link = persistent<string>(connector, { clock, jitter: 0, buffer: 2, overflow: 'oldest' })
    const got = collect(links[0] as Link<string>)
    expect(link.send('a')).toBe(true)
    expect(link.send('b')).toBe(true)
    expect(link.send('c')).toBe(true)
    links[0]?.send('ready')
    await settled()
    expect(got).toEqual(['b', 'c'])
  })

  it('drops buffered messages older than ttl even without onDrop', async () => {
    const clock = fakeClock()
    const links: Link<string>[] = []
    const connector = (): Link<string> => {
      const [client, server] = pair<string>()
      links.push(server)
      return client
    }
    const link = persistent<string>(connector, { clock, jitter: 0, timeout: 0, ttl: 1000 })
    const got = collect(links[0] as Link<string>)
    link.send('old')
    clock.advance(1500)
    expect(link.send('new')).toBe(true)
    links[0]?.send('ready')
    await settled()
    expect(got).toEqual(['new'])
  })

  it('reports a throwing connector and schedules another attempt', () => {
    const clock = fakeClock()
    const onError = vi.fn<(err: unknown) => void>()
    const onState = vi.fn<(state: Persistent.State) => void>()
    const boom = new Error('no runtime')
    const links: Link<string>[] = []
    let calls = 0
    const connector = (): Link<string> => {
      if (++calls === 1) throw boom
      const [client, server] = pair<string>()
      links.push(server)
      return client
    }
    const link = persistent<string>(connector, { clock, jitter: 0, onError, onState })
    expect(onError).toHaveBeenCalledExactlyOnceWith(boom)
    expect(link.state).toBe('retrying')
    expect(link.attempts).toBe(1)
    expect(onState.mock.calls.map(([s]) => s)).toEqual(['retrying'])
    expect(clock.pending).toBe(1)
    clock.advance(250)
    expect(calls).toBe(2)
    expect(links).toHaveLength(1)
    expect(link.state).toBe('connecting')
  })

  it('treats a connector that returns an already-closed link as a failed attempt', () => {
    const clock = fakeClock()
    const onState = vi.fn<(state: Persistent.State) => void>()
    const links: Link<string>[] = []
    const connector = vi.fn((): Link<string> => {
      const [client, server] = pair<string>()
      links.push(server)
      if (links.length === 1) client.close()
      return client
    })
    let link!: ReturnType<typeof persistent<string>>
    expect(() => {
      link = persistent<string>(connector, { clock, jitter: 0, timeout: 100, onState })
    }).not.toThrow()
    expect(link.state).toBe('retrying')
    expect(link.attempts).toBe(1)
    expect(onState.mock.calls.map(([s]) => s)).toEqual(['retrying'])
    expect(clock.pending).toBe(1)
    clock.advance(250)
    expect(connector).toHaveBeenCalledTimes(2)
    expect(link.state).toBe('connecting')
    expect(clock.pending).toBe(1)
  })

  it('drops buffered messages older than ttl', async () => {
    const r = rig({ timeout: 0, ttl: 1000 })
    const got = collect(r.server())
    expect(r.link.send('old')).toBe(true)
    r.clock.advance(1500)
    expect(r.link.send('new')).toBe(true)
    expect(r.drops()).toEqual(['old'])
    await r.greet()
    expect(got).toEqual(['new'])
  })

  it('expires stale buffered messages at flush time too', async () => {
    const r = rig({ timeout: 0, ttl: 1000 })
    const got = collect(r.server())
    r.link.send('old')
    r.clock.advance(1001)
    await r.greet()
    expect(r.drops()).toEqual(['old'])
    expect(got).toEqual([])
  })

  it('refuses the newest message when the buffer is full by default', async () => {
    const r = rig({ buffer: 2 })
    const got = collect(r.server())
    expect(r.link.send('a')).toBe(true)
    expect(r.link.send('b')).toBe(true)
    expect(r.link.send('c')).toBe(false)
    expect(r.drops()).toEqual(['c'])
    await r.greet()
    expect(got).toEqual(['a', 'b'])
  })

  it("sheds the oldest message when overflow is 'oldest'", async () => {
    const r = rig({ buffer: 2, overflow: 'oldest' })
    const got = collect(r.server())
    expect(r.link.send('a')).toBe(true)
    expect(r.link.send('b')).toBe(true)
    expect(r.link.send('c')).toBe(true)
    expect(r.drops()).toEqual(['a'])
    await r.greet()
    expect(got).toEqual(['b', 'c'])
  })

  it('never buffers with buffer: 0', async () => {
    const r = rig({ buffer: 0 })
    const got = collect(r.server())
    expect(r.link.send('a')).toBe(false)
    expect(r.drops()).toEqual(['a'])
    await r.greet()
    expect(got).toEqual([])
    expect(r.link.send('b')).toBe(true)
    await settled()
    expect(got).toEqual(['b'])
  })

  it('retryNow abandons the pending wait, reconnects immediately and resets attempts', async () => {
    const r = rig({ timeout: 100 })
    r.clock.advance(100)
    expect(r.link.state).toBe('retrying')
    expect(r.link.attempts).toBe(1)
    r.link.retryNow()
    expect(r.links).toHaveLength(2)
    expect(r.link.state).toBe('connecting')
    expect(r.link.attempts).toBe(0)
    expect(r.clock.pending).toBe(1)
    await r.greet()
    expect(r.link.state).toBe('open')
    expect(r.connector).toHaveBeenCalledTimes(2)
    expect(r.clock.pending).toBe(0)
  })

  it('retryNow is a no-op when open or closed', async () => {
    const r = rig()
    await r.greet()
    r.link.retryNow()
    expect(r.connector).toHaveBeenCalledOnce()
    expect(r.link.state).toBe('open')
    r.link.close()
    r.link.retryNow()
    expect(r.connector).toHaveBeenCalledOnce()
    expect(r.link.state).toBe('closed')
  })

  it('close drops the outbox to onDrop, closes the inner link and fires closed once', () => {
    const r = rig()
    const closed = vi.fn()
    const onClose = vi.fn()
    r.link.closed(closed)
    r.link.listen(vi.fn(), { onClose })
    r.link.send('q')
    r.link.close()
    expect(r.link.state).toBe('closed')
    expect(r.link.up).toBe(false)
    expect(r.drops()).toEqual(['q'])
    expect(r.server().up).toBe(false)
    expect(closed).toHaveBeenCalledOnce()
    expect(onClose).toHaveBeenCalledOnce()
    expect(r.clock.pending).toBe(0)
    expect(r.states()).toEqual(['closed'])
    r.link.close()
    expect(closed).toHaveBeenCalledOnce()
    expect(r.states()).toEqual(['closed'])
    expect(r.link.send('x')).toBe(false)
    expect(r.drops()).toEqual(['q'])
  })

  it('close while open closes the inner link and reports the transition', async () => {
    const r = rig()
    await r.greet()
    r.link.close()
    expect(r.server().up).toBe(false)
    expect(r.states()).toEqual(['open', 'closed'])
    expect(r.link.up).toBe(false)
  })

  it('Port onClose fires with no error on give-up, and not while merely retrying', () => {
    const r = rig({ timeout: 100, maxAttempts: 2 })
    const onClose = vi.fn<(error?: unknown) => void>()
    r.link.listen(vi.fn(), { onClose })
    r.clock.advance(100)
    expect(r.link.state).toBe('retrying')
    expect(onClose).not.toHaveBeenCalled()
    r.clock.advance(250)
    r.clock.advance(100)
    expect(r.link.state).toBe('closed')
    expect(onClose).toHaveBeenCalledOnce()
    expect(onClose.mock.calls[0]?.[0]).toBeUndefined()
  })

  it('listen onClose and closed fire synchronously on a closed persistent link', () => {
    const r = rig()
    r.link.close()
    const onClose = vi.fn()
    const closed = vi.fn()
    r.link.listen(vi.fn(), { onClose })
    r.link.closed(closed)
    expect(onClose).toHaveBeenCalledOnce()
    expect(closed).toHaveBeenCalledOnce()
  })

  it('remote and meta track the current inner link', () => {
    const r = rig({ timeout: 100 })
    expect(r.link.remote).toBe('server#1')
    expect(r.link.meta).toEqual({ n: 1 })
    r.clock.advance(100)
    r.clock.advance(250)
    expect(r.link.remote).toBe('server#2')
    expect(r.link.meta).toEqual({ n: 2 })
  })
})
