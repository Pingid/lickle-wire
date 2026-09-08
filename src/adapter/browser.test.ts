import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  asDuplex,
  asPort,
  asTarget,
  bridge,
  connector,
  fromWindow,
  handshake,
  link,
  asTransferPort,
  tunnel,
  worker,
  type Browser,
} from './browser.js'
import {
  mux,
  pair as linkPair,
  postOffer,
  readOffers,
  slot,
  type Accept,
  type Link,
  type Mux,
  type Transfer,
} from '../bus/link/index.ts'
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

const poster = (impl?: Browser.Post['postMessage']) => ({
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
const track = <L extends Link<unknown>>(one: L): L => {
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
    const like: Browser.Target = {
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

    const onLink = vi.fn<(one: Link<Msg>) => void>()
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
    const onLink = vi.fn<(one: Link<Msg>) => void>()
    handshake<Msg>(target, { heartbeat: 0 })(onLink)

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: '', ports: [port2] })

    expect(onLink).toHaveBeenCalledTimes(1)
    const one = track(onLink.mock.calls[0]?.[0] as Link<Msg>)
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
    const onLink = vi.fn<(one: Link<Msg>) => void>()
    handshake<Msg>(target, { heartbeat: 0 })(onLink)

    target.dispatch({
      data: { ...OPEN, name: 'app', path: ['https://inner.example'], meta: { role: 'panel', origin: 'spoofed' } },
      origin: '',
      ports: [port2],
    })

    const one = track(onLink.mock.calls[0]?.[0] as Link<Msg>)
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
    const onLink = vi.fn<(one: Link<Msg>) => void>()
    handshake<Msg>(target, { heartbeat: 0, origins: ['https://child.example'] })(onLink)

    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: 'https://child.example', ports: [port2] })

    expect(onLink).toHaveBeenCalledTimes(1)
    const one = track(onLink.mock.calls[0]?.[0] as Link<Msg>)
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

  it('closes the peers it accepted on teardown, not just its listener', () => {
    const target = events()
    const accepted: Link<Msg>[] = []
    const off = handshake<Msg>(target, { heartbeat: 0 })((one) => accepted.push(one))

    for (const name of ['a', 'b']) {
      target.dispatch({ data: { ...OPEN, name, path: [] }, origin: '', ports: [channel().port2] })
    }
    expect(accepted).toHaveLength(2)
    expect(accepted.every((one) => one.up)).toBe(true)

    // A source owns what it produced: stopping it disconnects its peers rather
    // than deregistering them and leaving the ports open.
    off()
    expect(accepted.every((one) => one.up)).toBe(false)
  })

  it('closes a port claimed from an event already in flight when it has stopped', () => {
    const target = events()
    const accepted: Link<Msg>[] = []
    let off = () => {}
    // A second listener on the same target stops the source mid-dispatch, so
    // the handshake below is claimed by a source that is already over.
    target.addEventListener('message', () => off())
    off = handshake<Msg>(target, { heartbeat: 0 })((one) => accepted.push(one))

    target.dispatch({ data: { ...OPEN, name: 'late', path: [] }, origin: '', ports: [channel().port2] })
    expect(accepted).toHaveLength(0)
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

    p.to.postMessage({ n: 1 })
    expect(target.postMessage).toHaveBeenCalledWith({ n: 1 }, 'https://a.example', [])

    const got = vi.fn<(e: MessageEvent) => void>()
    const off = p.from(got)
    on.dispatch({ data: { n: 1 }, origin: 'https://a.example', source: target })
    on.dispatch({ data: { n: 2 }, origin: 'https://b.example', source: target })
    on.dispatch({ data: { n: 3 }, origin: 'https://a.example', source: other })
    on.dispatch({ data: { n: 4 }, origin: 'https://a.example' })

    expect(got).toHaveBeenCalledTimes(1)
    expect(got.mock.calls[0]?.[0].data).toEqual({ n: 1 })

    off()
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
    p.from(got)

    p.to.postMessage({ n: 1 })
    current = second
    p.to.postMessage({ n: 2 })
    expect(first.postMessage).toHaveBeenCalledTimes(1)
    expect(second.postMessage).toHaveBeenCalledWith({ n: 2 }, 'https://a.example', [])

    on.dispatch({ data: { n: 1 }, origin: 'https://a.example', source: first })
    on.dispatch({ data: { n: 2 }, origin: 'https://a.example', source: second })
    expect(got).toHaveBeenCalledTimes(1)
    expect(got.mock.calls[0]?.[0].data).toEqual({ n: 2 })
  })
})

describe('asPort()', () => {
  it('carries messages both ways with no lifecycle and no heartbeat', async () => {
    const { port1, port2 } = channel()
    const a = asPort<Msg>(port1)
    const seen = tap(port2)

    const got = vi.fn<(m: Msg) => void>()
    a.listen(got)
    a.send({ n: 1 })
    port2.postMessage({ n: 2 })
    await flush()

    expect(seen).toEqual([{ n: 1 }])
    expect(got).toHaveBeenCalledWith({ n: 2 })
    // No ctrl traffic: a bare Port frames nothing.
    expect(seen.some((m) => typeof m === 'object' && m !== null && 'kind' in m)).toBe(false)
  })

  it('attaches on the first listener and detaches on the last', () => {
    const target = events()
    const a = asPort<Msg>({ to: poster(), from: target })
    expect(target.size).toBe(0)

    const one = a.listen(vi.fn())
    const two = a.listen(vi.fn())
    expect(target.size).toBe(1)

    one()
    expect(target.size).toBe(1)
    two()
    expect(target.size).toBe(0)

    // ...and re-attaches for a later subscriber.
    a.listen(vi.fn())
    expect(target.size).toBe(1)
  })

  it('honours an abort signal and never delivers to an already-aborted listener', () => {
    const target = events()
    const a = asPort<Msg>({ to: poster(), from: target })
    const got = vi.fn<(m: Msg) => void>()

    a.listen(got, { signal: AbortSignal.abort() })
    expect(target.size).toBe(0)

    const ac = new AbortController()
    a.listen(got, { signal: ac.signal })
    target.dispatch({ data: { n: 1 }, origin: '' })
    ac.abort()
    target.dispatch({ data: { n: 2 }, origin: '' })

    expect(got).toHaveBeenCalledTimes(1)
    expect(target.size).toBe(0)
  })

  it('never throws from send, and reports an uncloneable payload', () => {
    const onError = vi.fn()
    const a = asPort<any>(
      poster(() => {
        throw Object.assign(new Error('nope'), { name: 'DataCloneError' })
      }),
      { onError },
    )
    expect(() => a.send({ n: 1 })).not.toThrow()
    expect(onError).toHaveBeenCalledTimes(1)
  })

  it('isolates a throwing listener from the rest', () => {
    const onError = vi.fn()
    const target = events()
    const a = asPort<Msg>({ to: poster(), from: target }, { onError })
    const after = vi.fn<(m: Msg) => void>()
    a.listen(() => {
      throw new Error('boom')
    })
    a.listen(after)

    target.dispatch({ data: { n: 1 }, origin: '' })
    expect(after).toHaveBeenCalledWith({ n: 1 })
    expect(onError).toHaveBeenCalledTimes(1)
  })
})

describe('asTarget()', () => {
  /** A hand-driven Port standing in for a topic or an rpc side. */
  const stub = () => {
    const fns = new Set<(v: Msg) => void>()
    const sent: Msg[] = []
    let closed = false
    return {
      sent,
      deliver: (m: Msg) => fns.forEach((f) => f(m)),
      get closed() {
        return closed
      },
      port: {
        send: (m: Msg) => void sent.push(m),
        listen: (next: (m: Msg) => void) => {
          fns.add(next)
          return () => fns.delete(next)
        },
        close: () => {
          closed = true
        },
      },
    }
  }

  it('presents a Port as a DOM duplex, synthesising message events', () => {
    const s = stub()
    const t = asTarget<Msg>(s.port)

    const got = vi.fn<(e: MessageEvent) => void>()
    t.addEventListener('message', got)
    s.deliver({ n: 1 })
    expect(got.mock.calls[0]?.[0].data).toEqual({ n: 1 })

    t.postMessage({ n: 2 })
    expect(s.sent).toEqual([{ n: 2 }])

    t.removeEventListener('message', got)
    s.deliver({ n: 3 })
    expect(got).toHaveBeenCalledTimes(1)
  })

  it('forwards close to the source, so a Link keeps its lifecycle as a target', () => {
    const s = stub()
    const t = asTarget<Msg>(s.port)
    t.addEventListener('message', vi.fn())
    t.close?.()
    expect(s.closed).toBe(true)
  })

  it('round-trips: a Port presented as a target and read back as a Port', async () => {
    const s = stub()
    const back = asPort<Msg>(asTarget<Msg>(s.port))

    const got = vi.fn<(m: Msg) => void>()
    back.listen(got)
    s.deliver({ n: 1 })
    back.send({ n: 2 })

    expect(got).toHaveBeenCalledWith({ n: 1 })
    expect(s.sent).toEqual([{ n: 2 }])
  })

  it('is a target a full link can be built over', async () => {
    const { port1, port2 } = channel()
    // asTarget over a plain Port, then `link` on top: adapters stack.
    const one = track(link<Msg>(asTarget<Msg>(asPort<Msg>(port1)), { name: 'stacked', heartbeat: 0 }))
    const far = track(link<Msg>(port2, { heartbeat: 0 }))

    const got = vi.fn<(m: Msg) => void>()
    one.listen(got)
    far.send({ n: 1 })
    await flush()

    expect(one.remote).toBe('stacked')
    expect(got).toHaveBeenCalledWith({ n: 1 })
  })
})

describe('asTarget() transferables', () => {
  it('reports a transfer list it cannot carry, rather than dropping it silently', () => {
    const onError = vi.fn()
    const sent: unknown[] = []
    const t = asTarget<any>({ send: (m) => void sent.push(m), listen: () => () => {} }, { onError })

    const { port2 } = channel()
    t.postMessage({ kind: 'wire/open' }, [port2])

    // A `Port` has no transfer list. Saying so beats a handshake that never
    // completes and a peer that retries forever.
    expect(onError).toHaveBeenCalledTimes(1)
    expect(String(onError.mock.calls[0]?.[0])).toMatch(/transfer/i)
    expect(sent).toEqual([])
  })

  it('passes an empty or absent transfer list straight through', () => {
    const onError = vi.fn()
    const sent: unknown[] = []
    const t = asTarget<any>({ send: (m) => void sent.push(m), listen: () => () => {} }, { onError })

    t.postMessage({ n: 1 })
    t.postMessage({ n: 2 }, [])
    expect(sent).toEqual([{ n: 1 }, { n: 2 }])
    expect(onError).not.toHaveBeenCalled()
  })

  it('refuses to be the `up` of a handshake, because the port would not arrive', () => {
    const onError = vi.fn()
    const up = asTarget<any>({ send: () => {}, listen: () => () => {} }, { onError })
    const one = connector<Msg>('app', up, { heartbeat: 0 })()
    open.push(one)

    expect(onError).toHaveBeenCalledTimes(1)
  })
})

describe('tunnel()', () => {
  it('relays a frame handshake into a channel on a link, where no port could go', async () => {
    // The parent's next hop is a plain Link — a server, an extension, anything
    // that cannot take a transferred port. `bridge` is impossible here.
    const [nearWire, farWire] = linkPair<Mux.Wire>('parent', 'host')
    const upward = mux<Msg>(nearWire, { side: 'a' })
    const host = mux<Msg>(farWire, { side: 'b' })

    const arrived: Link<Msg>[] = []
    host.incoming((one) => arrived.push(one))

    const target = events()
    const stop = tunnel<Msg>(target, upward, { heartbeat: 0 })

    // A frame offers itself the ordinary way: an Open frame and a port.
    const { port1, port2 } = channel()
    target.dispatch({
      data: { ...OPEN, name: 'frame', path: [] },
      origin: '',
      ports: [port2],
    })
    await flush()

    expect(arrived).toHaveLength(1)
    const far = arrived[0] as Link<Msg>
    expect(far.remote).toBe('frame')
    // `relay` recorded the hop on the way through.
    expect(far.meta.path).toEqual([''])

    // Traffic flows frame <-> parent <-> host, with the parent in the middle.
    const atHost = vi.fn<(m: Msg) => void>()
    far.listen(atHost)
    const frame = track(link<Msg>(port1, { heartbeat: 0 }))
    frame.send({ n: 1 })
    await flush()
    expect(atHost).toHaveBeenCalledWith({ n: 1 })

    const atFrame = vi.fn<(m: Msg) => void>()
    frame.listen(atFrame)
    far.send({ n: 2 })
    await flush()
    expect(atFrame).toHaveBeenCalledWith({ n: 2 })

    // Closing one end of the tunnel takes the other with it.
    const ended = vi.fn()
    far.closed(ended)
    frame.close()
    await flush()
    expect(ended).toHaveBeenCalledTimes(1)

    stop()
  })

  it('vets offers exactly like bridge does', async () => {
    const [nearWire, farWire] = linkPair<Mux.Wire>('parent', 'host')
    const upward = mux<Msg>(nearWire, { side: 'a' })
    const host = mux<Msg>(farWire, { side: 'b' })
    const arrived: Link<Msg>[] = []
    host.incoming((one) => arrived.push(one))

    const target = events()
    tunnel<Msg>(target, upward, {
      heartbeat: 0,
      origins: ['https://allowed.example'],
      filter: (o) => o.name === 'wanted',
    })

    const declined = channel()
    target.dispatch({
      data: { ...OPEN, name: 'wanted', path: [] },
      origin: 'https://other.example',
      ports: [declined.port2],
    })
    target.dispatch({
      data: { ...OPEN, name: 'other', path: [] },
      origin: 'https://allowed.example',
      ports: [channel().port2],
    })
    await flush()
    expect(arrived).toHaveLength(0)

    target.dispatch({
      data: { ...OPEN, name: 'wanted', path: [] },
      origin: 'https://allowed.example',
      ports: [channel().port2],
    })
    await flush()
    expect(arrived).toHaveLength(1)
    expect(arrived[0]?.meta.origin).toBe('https://allowed.example')
  })
})

describe('relaying to a worker held in a slot', () => {
  it('bridges to whichever worker is bound, through a sink that follows the slot', async () => {
    // A Worker can take a transferred port, so the page should hand handshakes
    // straight to it and stay out of the data path. The sink is indirect, so
    // the relay is wired once and always aims at the current worker.
    const wire = slot<Msg>({ own: true })
    const to: Browser.Sink = {
      postMessage: (m, t) => current?.postMessage(m, t ?? []),
    }
    let current: MessagePort | null = null

    const target = events()
    const stop = bridge(target, to, { heartbeat: 0 })

    /** Stands in for `new Worker(...)`: port1 is the page's end. */
    const spawn = () => {
      const c = channel()
      current = c.port1
      wire.use(link<Msg>(c.port1, { heartbeat: 0, own: true }))
      // The worker's end, where a hub would run `handshake(workerSelf())`.
      const accepted: Link<Msg>[] = []
      handshake<Msg>(c.port2, { heartbeat: 0 })((one) => accepted.push(one))
      c.port2.start()
      return accepted
    }

    const first = spawn()
    const frameA = channel()
    target.dispatch({ data: { ...OPEN, name: 'frame-a', path: [] }, origin: '', ports: [frameA.port2] })
    await flush()
    expect(first.map((l) => l.remote)).toEqual(['frame-a'])

    // Swap the worker. The relay is untouched; the sink follows.
    const second = spawn()
    const frameB = channel()
    target.dispatch({ data: { ...OPEN, name: 'frame-b', path: [] }, origin: '', ports: [frameB.port2] })
    await flush()

    expect(second.map((l) => l.remote)).toEqual(['frame-b'])
    expect(first).toHaveLength(1) // the old worker saw nothing new
    stop()
  })

  it('tunnels through the slot when the next hop cannot take a port', async () => {
    // Same page, but the hub is behind a plain Link — a socket, an extension.
    // Frames survive a swap without reconnecting, at the cost of the page
    // carrying their traffic.
    const wire = slot<Mux.Wire>({ own: true })
    const up = mux<Msg>(wire, { side: 'a' })
    const target = events()
    tunnel<Msg>(target, up, { heartbeat: 0 })

    const boot = () => {
      const [page, host] = linkPair<Mux.Wire>('page', 'host')
      const far = mux<Msg>(host, { side: 'b' })
      const arrived: Link<Msg>[] = []
      far.incoming((one) => arrived.push(one))
      return { page, arrived }
    }

    const first = boot()
    wire.use(first.page)
    const frame = channel()
    target.dispatch({ data: { ...OPEN, name: 'frame', path: [] }, origin: '', ports: [frame.port2] })
    await flush()
    expect(first.arrived).toHaveLength(1)

    // Swap the host. The frame's channel is announced to the new one.
    const second = boot()
    wire.use(second.page)
    await flush()
    expect(second.arrived.map((l) => l.remote)).toEqual(['frame'])

    // ...and the frame, which never reconnected, still reaches it.
    const got = vi.fn<(m: Msg) => void>()
    second.arrived[0]?.listen(got)
    const near = track(link<Msg>(frame.port1, { heartbeat: 0 }))
    near.send({ n: 7 })
    await flush()
    expect(got).toHaveBeenCalledWith({ n: 7 })
  })
})

describe('the transfer layer', () => {
  it('drives the same handshake over a AsasTransferPort instead of a DOM target', async () => {
    const target = events()
    const accepted: Link<Msg>[] = []
    // Explicitly through the port form: what a non-DOM platform would pass.
    handshake<Msg>(asTransferPort<Transfer.Open>({ to: poster(), from: target }), {
      heartbeat: 0,
      origins: ['https://a.example'],
    })((one) => accepted.push(one))

    const { port2 } = channel()
    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: 'https://a.example', ports: [port2] })

    expect(accepted).toHaveLength(1)
    expect(accepted[0]?.remote).toBe('app')
    expect(accepted[0]?.meta.origin).toBe('https://a.example')
  })

  it('unwraps the frame into postMessage(data, transfer)', () => {
    const to = poster()
    const up = asTransferPort<Transfer.Open>({ to, from: events() })
    const { port2 } = channel()

    up.send({ data: { kind: 'wire/open', v: 1, name: 'app', path: [] }, transfer: [port2] })
    expect(to.postMessage).toHaveBeenCalledWith({ kind: 'wire/open', v: 1, name: 'app', path: [] }, [port2])

    // Nothing to hand over: one argument, so a Window sink never reads a port
    // list as a target origin.
    up.send({ data: { kind: 'wire/open', v: 1, name: 'app', path: [] } })
    expect(to.postMessage.mock.calls[1]).toHaveLength(1)
  })

  it('packs ports and origin off the event, and ignores frames that are not offers', () => {
    const target = events()
    const up = asTransferPort<Transfer.Open>({ to: poster(), from: target })
    const got: Transfer.In<Transfer.Open, MessagePort>[] = []
    up.listen((f) => got.push(f))

    const { port2 } = channel()
    target.dispatch({ data: { ...OPEN, name: 'app', path: [] }, origin: 'https://a.example', ports: [port2] })
    expect(got[0]?.transfer).toEqual([port2])
    expect(got[0]?.origin).toBe('https://a.example')

    // Envelopes and control frames still arrive; `readOffers` is what filters.
    target.dispatch({ data: { $: 'app', v: 2, t: 'ready', id: 'p1' }, origin: '' })
    const seen: Link<Msg>[] = []
    handshake<Msg>(target, { heartbeat: 0 })((one) => seen.push(one))
    target.dispatch({ data: { $: 'app', v: 2, t: 'ready', id: 'p1' }, origin: '' })
    expect(seen).toHaveLength(0)
  })

  it('is the same protocol over a channel type that is not a MessagePort', async () => {
    // The cross-platform claim: an extension runtime port, a socket, anything.
    type Chan = { id: number }
    const [near, far] = linkPair<unknown>('client', 'server')

    const asOffers = (l: Link<unknown>): Transfer.Offers<Chan> => ({
      send: (msg) => void l.send({ data: msg.data, transfer: msg.transfer, origin: 'ext://self' }),
      listen: (next, o) => l.listen((m) => next(m as Transfer.In<Transfer.Open, Chan>), o),
    })

    const seen: Accept.Offer<Chan>[] = []
    readOffers(asOffers(far))((o) => seen.push(o))
    postOffer(asOffers(near), 'panel', { id: 7 }, { role: 'sidebar' })
    await flush()

    expect(seen).toHaveLength(1)
    expect(seen[0]?.name).toBe('panel')
    expect(seen[0]?.channel).toEqual({ id: 7 })
    expect(seen[0]?.origin).toBe('ext://self')
    expect(seen[0]?.claimed).toEqual({ role: 'sidebar' })
  })
})

describe('asDuplex()', () => {
  it('is a Link and a target at once, over one connection', async () => {
    const [near, far] = linkPair<Msg>('near', 'far')
    const both = asDuplex(near)

    // Both faces are the real types, not lookalikes.
    const asLinkFace: Link<Msg> = both
    const asTargetFace: Browser.Duplex = both
    void [asLinkFace, asTargetFace]

    expect(both.remote).toBe('far')
    expect(both.up).toBe(true)

    // Receive through the DOM face...
    const domSide = vi.fn<(e: MessageEvent) => void>()
    both.addEventListener('message', domSide)
    far.send({ n: 1 })
    await flush()
    expect(domSide.mock.calls[0]?.[0].data).toEqual({ n: 1 })

    // ...and through the wire face, at the same time.
    const wireSide = vi.fn<(m: Msg) => void>()
    both.listen(wireSide)
    far.send({ n: 2 })
    await flush()
    expect(wireSide).toHaveBeenCalledWith({ n: 2 })

    // Send either way.
    const atFar = vi.fn<(m: Msg) => void>()
    far.listen(atFar)
    both.postMessage({ n: 3 })
    both.send({ n: 4 })
    await flush()
    expect(atFar.mock.calls.map((c) => c[0])).toEqual([{ n: 3 }, { n: 4 }])
  })

  it('tracks a far end that moves, rather than snapshotting it', () => {
    const wire = slot<Msg>({ remote: 'idle' })
    const both = asDuplex(wire)
    expect(both.remote).toBe('idle')
    expect(both.up).toBe(false)

    const [near] = linkPair<Msg>('near', 'worker-1')
    wire.use(near)
    expect(both.remote).toBe('worker-1')
    expect(both.up).toBe(true)
  })

  it('reports liveness through the link face while the DOM face stays usable', async () => {
    const [near, far] = linkPair<Msg>('near', 'far')
    const both = asDuplex(near)
    const ended = vi.fn()
    both.closed(ended)

    far.close()
    await flush()
    expect(ended).toHaveBeenCalledTimes(1)
    expect(both.up).toBe(false)
  })

  it('closes the one connection, whichever face is used', async () => {
    const [a, aFar] = linkPair<Msg>('a', 'aFar')
    const viaLink = asDuplex(a)
    viaLink.close()
    await flush()
    expect(a.up).toBe(false)
    expect(aFar.up).toBe(false)

    const [b, bFar] = linkPair<Msg>('b', 'bFar')
    const viaTarget: Browser.Duplex = asDuplex(b)
    viaTarget.close?.()
    await flush()
    expect(b.up).toBe(false)
    expect(bFar.up).toBe(false)
  })

  it('is a target other adapters can be built over', async () => {
    const [near, far] = linkPair<Msg>('near', 'far')
    // A mux channel or a slot handed to something that only speaks postMessage.
    const stacked = track(link<Msg>(asDuplex(near), { name: 'stacked', heartbeat: 0 }))

    const got = vi.fn<(m: Msg) => void>()
    stacked.listen(got)
    far.send({ n: 1 })
    await flush()
    expect(stacked.remote).toBe('stacked')
    expect(got).toHaveBeenCalledWith({ n: 1 })
  })

  it('still refuses a transfer list, because a link has no way to carry one', () => {
    const onError = vi.fn()
    const [near] = linkPair<Msg>('near', 'far')
    const both = asDuplex(near, { onError })
    both.postMessage({ n: 1 } as unknown as Msg, [channel().port2])
    expect(onError).toHaveBeenCalledTimes(1)
  })
})
