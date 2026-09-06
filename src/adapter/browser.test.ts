import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { bridge, connector, fromWindow, handshake, link, worker, type Link } from './browser.js'
import type { ILink } from '../bus/link/index.ts'
import { fakeClock } from '../bus/testing.ts'

// --- harness ----------------------------------------------------------------

type Msg = { n: number }

/** `MessagePort` delivery is asynchronous in Node; this outlasts one hop. */
const flush = () => new Promise<void>((r) => setTimeout(r, 5))

const OPEN = { kind: 'wire/open', v: 1 } as const

/** A `MessageEvents` target we can dispatch fake `MessageEvent`s into. */
const events = () => {
  const ls = new Set<(event: MessageEvent) => void>()
  const target: {
    addEventListener: (type: 'message', fn: (event: MessageEvent) => void) => void
    removeEventListener: (type: 'message', fn: (event: MessageEvent) => void) => void
    readonly size: number
    dispatch(event: { data: unknown; origin: string; source?: unknown; ports?: readonly MessagePort[] }): void
  } = {
    addEventListener: (_type, fn) => {
      ls.add(fn)
    },
    removeEventListener: (_type, fn) => {
      ls.delete(fn)
    },
    get size() {
      return ls.size
    },
    dispatch: (event) => {
      const e = { source: null, ports: [], ...event } as unknown as MessageEvent
      for (const fn of [...ls]) fn(e)
    },
  }
  return target
}

const poster = (impl?: Link.Target['postMessage']) => ({
  postMessage: impl ? vi.fn(impl) : vi.fn<(message: unknown, transfer?: Transferable[]) => void>(),
  addEventListener: () => {},
  removeEventListener: () => {},
})

/** Everything a raw `MessagePort` receives. Started so events flow. */
const tap = (port: MessagePort) => {
  const seen: unknown[] = []
  port.addEventListener('message', (e: MessageEvent) => seen.push(e.data))
  port.start()
  return seen
}

/** Ports and links opened by a test, closed in `afterEach` so vitest exits cleanly. */
const open: { close(): void }[] = []
const channel = () => {
  const c = new MessageChannel()
  open.push(c.port1, c.port2)
  return c
}
const track = <L extends ILink<unknown>>(one: L): L => {
  open.push(one)
  return one
}

afterEach(() => {
  for (const o of open.splice(0)) o.close()
})

// ----------------------------------------------------------------------------

describe('link()', () => {
  it('round-trips messages both ways over a MessageChannel with heartbeat: 0', async () => {
    const { port1, port2 } = channel()
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 0 }))
    const b = track(link<Msg>(port2, { name: 'a', heartbeat: 0 }))
    const gotA = vi.fn<(m: Msg) => void>()
    const gotB = vi.fn<(m: Msg) => void>()
    a.listen(gotA)
    b.listen(gotB)

    expect(a.remote).toBe('b')
    expect(b.remote).toBe('a')
    expect(a.send({ n: 1 })).toBe(true)
    expect(b.send({ n: 2 })).toBe(true)
    await flush()

    expect(gotB).toHaveBeenCalledTimes(1)
    expect(gotB).toHaveBeenCalledWith({ n: 1 })
    expect(gotA).toHaveBeenCalledTimes(1)
    expect(gotA).toHaveBeenCalledWith({ n: 2 })
  })

  it('pings on the heartbeat and stays up when the far end pongs', async () => {
    const clock = fakeClock()
    const { port1, port2 } = channel()
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 100, timeout: 50, clock }))
    track(link<Msg>(port2, { name: 'a', heartbeat: 0 }))
    const closed = vi.fn()
    a.closed(closed)

    expect(clock.pending).toBe(1)
    clock.advance(100)
    // The ping is out and the pong deadline is armed.
    expect(clock.pending).toBe(1)
    await flush()

    expect(a.up).toBe(true)
    expect(closed).not.toHaveBeenCalled()
    // The pong cancelled the deadline and re-armed the next beat.
    expect(clock.pending).toBe(1)
  })

  it('shuts and fires closed when the pong misses the deadline', async () => {
    const clock = fakeClock()
    const { port1, port2 } = channel()
    const pings = tap(port2)
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 100, timeout: 50, clock }))
    const closed = vi.fn()
    const changed = vi.fn<(up: boolean) => void>()
    a.closed(closed)
    a.changed(changed)

    clock.advance(100)
    expect(a.up).toBe(true)
    clock.advance(50)

    expect(a.up).toBe(false)
    expect(closed).toHaveBeenCalledTimes(1)
    expect(changed).toHaveBeenCalledWith(false)
    expect(clock.pending).toBe(0)
    await flush()
    expect(pings).toEqual([{ kind: 'wire/ctrl', c: 'ping', id: 1 }])
  })

  it('keeps pinging without a deadline when timeout is 0', async () => {
    const clock = fakeClock()
    const { port1, port2 } = channel()
    const seen = tap(port2)
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 100, timeout: 0, clock }))

    clock.advance(100)
    expect(clock.pending).toBe(1)
    clock.advance(1000)

    expect(a.up).toBe(true)
    expect(clock.pending).toBe(1)
    await flush()
    expect(seen).toHaveLength(11)
    expect(seen.every((m) => (m as { c: string }).c === 'ping')).toBe(true)
  })

  it('does not double-arm the beat when a pong arrives in keepalive mode', async () => {
    const clock = fakeClock()
    const { port1, port2 } = channel()
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 100, timeout: 0, clock }))
    track(link<Msg>(port2, { name: 'a', heartbeat: 0 }))

    clock.advance(100)
    await flush()

    expect(a.up).toBe(true)
    expect(clock.pending).toBe(1)
  })

  it('arms nothing when heartbeat is 0', () => {
    const clock = fakeClock()
    const { port1 } = channel()
    track(link<Msg>(port1, { name: 'b', heartbeat: 0, clock }))
    expect(clock.pending).toBe(0)
  })

  it('sends bye on close so the far end shuts without waiting out a heartbeat', async () => {
    const { port1, port2 } = channel()
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 0 }))
    const b = track(link<Msg>(port2, { name: 'a', heartbeat: 0 }))
    const closedB = vi.fn()
    b.closed(closedB)

    a.close()
    expect(a.up).toBe(false)
    expect(a.send({ n: 1 })).toBe(false)
    expect(b.up).toBe(true)
    await flush()

    expect(b.up).toBe(false)
    expect(closedB).toHaveBeenCalledTimes(1)
  })

  it('reports a DataCloneError without closing the link', async () => {
    const { port1, port2 } = channel()
    const onError = vi.fn<(err: unknown) => void>()
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 0, onError }))
    const b = track(link<Msg>(port2, { name: 'a', heartbeat: 0 }))
    const gotB = vi.fn<(m: Msg) => void>()
    b.listen(gotB)

    expect(a.send((() => {}) as unknown as Msg)).toBe(false)
    expect(onError).toHaveBeenCalledTimes(1)
    const err = onError.mock.calls[0]?.[0]
    expect(err).toBeInstanceOf(Error)
    expect((err as Error).name).toBe('DataCloneError')
    expect(a.up).toBe(true)

    expect(a.send({ n: 1 })).toBe(true)
    await flush()
    expect(gotB).toHaveBeenCalledWith({ n: 1 })
  })

  it('answers control traffic itself and never hands it to listen', async () => {
    const { port1, port2 } = channel()
    const a = track(link<Msg>(port1, { name: 'b', heartbeat: 0 }))
    const got = vi.fn<(m: Msg) => void>()
    a.listen(got)
    const seen = tap(port2)

    port2.postMessage({ kind: 'wire/ctrl', c: 'ping', id: 7 })
    port2.postMessage({ n: 1 })
    await flush()

    expect(got).toHaveBeenCalledTimes(1)
    expect(got).toHaveBeenCalledWith({ n: 1 })
    expect(seen).toEqual([{ kind: 'wire/ctrl', c: 'pong', id: 7 }])
  })

  it('calls start() when the target has one and detaches on close', () => {
    const target = events()
    const start = vi.fn()
    const posted: unknown[] = []
    const like: Link.Target = {
      postMessage: (m) => {
        posted.push(m)
      },
      addEventListener: target.addEventListener,
      removeEventListener: target.removeEventListener,
      start,
    }
    const one = track(link<Msg>(like, { heartbeat: 0 }))
    const got = vi.fn<(m: Msg) => void>()

    expect(start).toHaveBeenCalledTimes(1)
    expect(one.send({ n: 0 })).toBe(true)
    expect(posted).toEqual([{ n: 0 }])

    one.listen(got)
    expect(target.size).toBe(1)
    target.dispatch({ data: { n: 1 }, origin: '' })
    expect(got).toHaveBeenCalledWith({ n: 1 })
    one.close()
    expect(target.size).toBe(0)
  })
})

describe('worker()', () => {
  class FakeWorker {
    static made: FakeWorker[] = []
    terminated = false
    readonly listeners = new Set<(event: MessageEvent) => void>()
    posted: unknown[] = []
    constructor(
      readonly url: string | URL,
      readonly opts?: { type?: string },
    ) {
      FakeWorker.made.push(this)
    }
    postMessage(message: unknown) {
      this.posted.push(message)
    }
    addEventListener(_type: 'message', fn: (event: MessageEvent) => void) {
      this.listeners.add(fn)
    }
    removeEventListener(_type: 'message', fn: (event: MessageEvent) => void) {
      this.listeners.delete(fn)
    }
    terminate() {
      this.terminated = true
    }
  }

  beforeEach(() => {
    FakeWorker.made = []
    vi.stubGlobal('Worker', FakeWorker)
  })
  afterEach(() => vi.unstubAllGlobals())

  it('spawns the worker when served, not when the source is built', () => {
    const src = worker<Msg>('./a.ts', { heartbeat: 0 })
    expect(FakeWorker.made).toHaveLength(0)

    const onLink = vi.fn<(one: ILink<Msg>) => void>()
    src(onLink)
    expect(FakeWorker.made).toHaveLength(1)
    expect(FakeWorker.made[0]?.url).toBe('./a.ts')
    expect(FakeWorker.made[0]?.opts).toEqual({ type: 'module' })
    expect(onLink).toHaveBeenCalledTimes(1)
  })

  it('terminates the worker on teardown, because it created it', () => {
    const off = worker<Msg>('./a.ts', { heartbeat: 0 })(() => {})
    const made = FakeWorker.made[0] as FakeWorker
    expect(made.terminated).toBe(false)
    off()
    expect(made.terminated).toBe(true)
    expect(made.listeners.size).toBe(0)
  })

  it('passes the classic worker type through', () => {
    worker<Msg>('./a.ts', { type: 'classic', heartbeat: 0 })(() => {})
    expect(FakeWorker.made[0]?.opts).toEqual({ type: 'classic' })
  })
})

describe('connector()', () => {
  it('mints a fresh channel per attempt and posts the handshake with port2 in the transfer list', async () => {
    const up = poster()
    const connect = connector<Msg>('app', up, { heartbeat: 0 })
    expect(up.postMessage).not.toHaveBeenCalled()

    const l1 = track(connect())
    const l2 = track(connect())
    expect(up.postMessage).toHaveBeenCalledTimes(2)

    const [open1, transfer1] = up.postMessage.mock.calls[0] as [unknown, Transferable[]]
    const [open2, transfer2] = up.postMessage.mock.calls[1] as [unknown, Transferable[]]
    expect(open1).toEqual({ ...OPEN, name: 'app', path: [], meta: undefined })
    expect(open2).toEqual({ ...OPEN, name: 'app', path: [], meta: undefined })
    expect(transfer1).toHaveLength(1)
    expect(transfer1[0]).toBeInstanceOf(MessagePort)
    expect(transfer2[0]).toBeInstanceOf(MessagePort)
    expect(transfer1[0]).not.toBe(transfer2[0])
    expect(l1.remote).toBe('app')
    expect(l2.remote).toBe('app')

    // The transferred end is paired with the link that was returned.
    const far1 = transfer1[0] as MessagePort
    const far2 = transfer2[0] as MessagePort
    open.push(far1, far2)
    const seen1 = tap(far1)
    const seen2 = tap(far2)
    l1.send({ n: 1 })
    await flush()
    expect(seen1).toEqual([{ n: 1 }])
    expect(seen2).toEqual([])
  })

  it('forwards meta in the handshake', () => {
    const up = poster()
    const meta = { role: 'panel' }
    track(connector<Msg>('app', up, { heartbeat: 0, meta })())
    expect(up.postMessage.mock.calls[0]?.[0]).toEqual({ ...OPEN, name: 'app', path: [], meta })
  })
})

describe('handshake()', () => {
  it('claims a matching handshake and yields a link with origin and path in meta', async () => {
    const target = events()
    const { port1, port2 } = channel()
    const onLink = vi.fn<(one: ILink<Msg>) => void>()
    handshake<Msg>(target, { heartbeat: 0 })(onLink)

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: '', ports: [port2] })

    expect(onLink).toHaveBeenCalledTimes(1)
    const one = track(onLink.mock.calls[0]?.[0] as ILink<Msg>)
    expect(one.remote).toBe('app')
    expect(one.meta['origin']).toBe('')
    expect(one.meta['path']).toEqual([''])

    // The link is wired to the transferred port.
    const got = vi.fn<(m: Msg) => void>()
    one.listen(got)
    const far = track(link<Msg>(port1, { name: 'app', heartbeat: 0 }))
    far.send({ n: 1 })
    await flush()
    expect(got).toHaveBeenCalledWith({ n: 1 })
  })

  it('merges handshake meta under the origin facts', () => {
    const target = events()
    const { port2 } = channel()
    const onLink = vi.fn<(one: ILink<Msg>) => void>()
    handshake<Msg>(target, { heartbeat: 0 })(onLink)

    target.dispatch({
      data: { ...OPEN, name: 'app', path: ['https://inner.example'], meta: { role: 'panel', origin: 'spoofed' } },
      origin: '',
      ports: [port2],
    })

    const one = track(onLink.mock.calls[0]?.[0] as ILink<Msg>)
    expect(one.meta).toEqual({ role: 'panel', origin: '', path: ['', 'https://inner.example'] })
  })

  it('leaves the port untouched when filter declines', async () => {
    const target = events()
    const { port1, port2 } = channel()
    const onLink = vi.fn()
    const filter = vi.fn(() => false)
    handshake<Msg>(target, { heartbeat: 0, filter })(onLink)

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: '', ports: [port2] })

    expect(filter).toHaveBeenCalledWith({ name: 'app', origin: '', path: [] })
    expect(onLink).not.toHaveBeenCalled()
    // Still usable by whoever else wants it.
    const seen = tap(port2)
    port1.postMessage({ n: 1 })
    await flush()
    expect(seen).toEqual([{ n: 1 }])
  })

  it('ignores handshakes from an origin it was not told to allow', () => {
    const target = events()
    const { port2 } = channel()
    const onLink = vi.fn()
    handshake<Msg>(target, { heartbeat: 0 })(onLink)

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: 'https://evil.example', ports: [port2] })
    expect(onLink).not.toHaveBeenCalled()
  })

  it('accepts a named cross-origin frame', () => {
    const target = events()
    const { port2 } = channel()
    const onLink = vi.fn<(one: ILink<Msg>) => void>()
    handshake<Msg>(target, { heartbeat: 0, origins: ['https://child.example'] })(onLink)

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: 'https://child.example', ports: [port2] })

    expect(onLink).toHaveBeenCalledTimes(1)
    const one = track(onLink.mock.calls[0]?.[0] as ILink<Msg>)
    expect(one.meta['origin']).toBe('https://child.example')
  })

  it('ignores events that are not handshakes or carry no port', () => {
    const target = events()
    const onLink = vi.fn()
    handshake<Msg>(target, { heartbeat: 0 })(onLink)

    target.dispatch({ data: { n: 1 }, origin: '' })
    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: '' })
    target.dispatch({ data: { kind: 'wire/open', v: 2, name: 'app', path: [] }, origin: '', ports: [channel().port2] })
    expect(onLink).not.toHaveBeenCalled()
  })

  it('reports a throwing filter and declines', () => {
    const target = events()
    const { port2 } = channel()
    const onLink = vi.fn()
    const onError = vi.fn<(err: unknown) => void>()
    const boom = new Error('bad filter')
    handshake<Msg>(target, {
      heartbeat: 0,
      onError,
      filter: () => {
        throw boom
      },
    })(onLink)

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: '', ports: [port2] })

    expect(onError).toHaveBeenCalledWith(boom)
    expect(onLink).not.toHaveBeenCalled()
  })

  it('removes its listener on teardown and on signal abort', () => {
    const target = events()
    const off = handshake<Msg>(target, { heartbeat: 0 })(vi.fn())
    expect(target.size).toBe(1)
    off()
    expect(target.size).toBe(0)
    off()
    expect(target.size).toBe(0)

    const ac = new AbortController()
    handshake<Msg>(target, { heartbeat: 0, signal: ac.signal })(vi.fn())
    expect(target.size).toBe(1)
    ac.abort()
    expect(target.size).toBe(0)

    handshake<Msg>(target, { heartbeat: 0, signal: AbortSignal.abort() })(vi.fn())
    expect(target.size).toBe(0)
  })
})

describe('bridge()', () => {
  it('re-posts the handshake one hop up with the origin prepended to path and the port transferred', () => {
    const target = events()
    const { port2 } = channel()
    const up = poster()
    bridge(target, up, { origins: ['https://child.example'] })

    target.dispatch({
      data: { ...OPEN, name: 'app', path: ['https://inner.example'] },
      origin: 'https://child.example',
      ports: [port2],
    })

    expect(up.postMessage).toHaveBeenCalledTimes(1)
    const [openMsg, transfer] = up.postMessage.mock.calls[0] as [unknown, Transferable[]]
    expect(openMsg).toEqual({ ...OPEN, name: 'app', path: ['https://child.example', 'https://inner.example'] })
    // Identity, not deep equality: a Node `MessagePort` is cyclic and overflows a structural compare.
    expect(transfer).toHaveLength(1)
    expect(transfer[0]).toBe(port2)
  })

  it('declines by filter and origin like handshake', () => {
    const target = events()
    const up = poster()
    bridge(target, up, { filter: (h) => h.name === 'app' })

    target.dispatch({ data: { ...OPEN, name: 'other', path: [] }, origin: '', ports: [channel().port2] })
    target.dispatch({
      data: { ...OPEN, name: 'app', path: [] },
      origin: 'https://evil.example',
      ports: [channel().port2],
    })
    expect(up.postMessage).not.toHaveBeenCalled()

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: '', ports: [channel().port2] })
    expect(up.postMessage).toHaveBeenCalledTimes(1)
  })

  it('reports a poster that throws instead of letting it escape the message handler', () => {
    const target = events()
    const onError = vi.fn<(err: unknown) => void>()
    const boom = new Error('port already started')
    bridge(
      target,
      poster(() => {
        throw boom
      }),
      { onError },
    )

    expect(() =>
      target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: '', ports: [channel().port2] }),
    ).not.toThrow()
    expect(onError).toHaveBeenCalledWith(boom)
  })

  it('removes its listener on teardown and on signal abort', () => {
    const target = events()
    const off = bridge(target, poster())
    expect(target.size).toBe(1)
    off()
    expect(target.size).toBe(0)

    const ac = new AbortController()
    bridge(target, poster(), { signal: ac.signal })
    expect(target.size).toBe(1)
    ac.abort()
    expect(target.size).toBe(0)
  })
})

describe('fromWindow()', () => {
  it('posts with the origin and delivers only events from that origin and source', () => {
    const on = events()
    const target = { postMessage: vi.fn<(m: unknown, targetOrigin: string, transfer?: Transferable[]) => void>() }
    const other = { postMessage: vi.fn<(m: unknown, targetOrigin: string, transfer?: Transferable[]) => void>() }
    const p = fromWindow(() => target, 'https://a.example', { on: on })

    p.postMessage({ n: 1 })
    expect(target.postMessage).toHaveBeenCalledWith({ n: 1 }, 'https://a.example', [])

    const got = vi.fn<(e: MessageEvent) => void>()
    p.addEventListener('message', got)
    on.dispatch({ data: { n: 1 }, origin: 'https://a.example', source: target })
    on.dispatch({ data: { n: 2 }, origin: 'https://b.example', source: target })
    on.dispatch({ data: { n: 3 }, origin: 'https://a.example', source: other })
    on.dispatch({ data: { n: 4 }, origin: 'https://a.example' })

    expect(got).toHaveBeenCalledTimes(1)
    expect(got.mock.calls[0]?.[0].data).toEqual({ n: 1 })

    p.removeEventListener('message', got)
    expect(on.size).toBe(0)
    on.dispatch({ data: { n: 5 }, origin: 'https://a.example', source: target })
    expect(got).toHaveBeenCalledTimes(1)
  })

  it('resolves the target on every call', () => {
    const on = events()
    const first = { postMessage: vi.fn<(m: unknown, targetOrigin: string, transfer?: Transferable[]) => void>() }
    const second = { postMessage: vi.fn<(m: unknown, targetOrigin: string, transfer?: Transferable[]) => void>() }
    let current = first
    const p = fromWindow(() => current, 'https://a.example', { on: on })
    const got = vi.fn<(e: MessageEvent) => void>()
    p.addEventListener('message', got)

    p.postMessage({ n: 1 })
    current = second
    p.postMessage({ n: 2 })
    expect(first.postMessage).toHaveBeenCalledTimes(1)
    expect(second.postMessage).toHaveBeenCalledWith({ n: 2 }, 'https://a.example', [])

    on.dispatch({ data: { n: 1 }, origin: 'https://a.example', source: first })
    on.dispatch({ data: { n: 2 }, origin: 'https://a.example', source: second })
    expect(got).toHaveBeenCalledTimes(1)
    expect(got.mock.calls[0]?.[0].data).toEqual({ n: 2 })
  })
})
