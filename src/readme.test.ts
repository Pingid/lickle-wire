/**
 * Mirrors the code samples in README.md so they cannot drift from the API.
 *
 * Runnable samples assert the documented output. Samples that need a browser
 * runtime live in `browserSamples`, which is never called but is still
 * type-checked by `tsc --noEmit`.
 */
import { describe, expect, it } from 'vitest'

import {
  asDuplex,
  asPort,
  asTarget,
  bridge,
  connector,
  fromWindow,
  handshake,
  link,
  worker,
  workerSelf,
  type Browser,
} from './adapter/browser.ts'
import { accept, asLink, defineLink, defineSource, type Link } from './adapter/index.ts'
import { define, Hub, Session, mux, pair as linkPair, persistent, slot, type Envelope, type Mux } from './index.ts'
import * as replica from './replica/index.ts'
import { both, pair, recv, send, stream, symmetric, type Frame } from './rpc/index.ts'
import { fakeClock } from './bus/testing.ts'

const settled = () => new Promise<void>((r) => setTimeout(r, 0))

// --- shared.ts --------------------------------------------------------------

type Bus = { tick: number; api: Frame }

/** The `define` sample from the Session section: one declaration, both realms. */
const app = define<Bus>({ name: 'app', retain: ['tick'] })

const api = pair('api', {
  getUser: send<{ id: string }>().reply<{ id: string; name: string }>(),
  watch: stream<{ q: string }, number>(),
})

describe('README', () => {
  it('Quick Start, in one process', async () => {
    const hub = Hub.create<Bus>('app', { retain: ['tick'] })
    const a = hub.local()
    const b = hub.local()
    await Promise.all([a.ready(), b.ready()])

    const log: [number, boolean][] = []
    hub.publish('tick', 1)
    b.topic('tick').listen((t, meta) => log.push([t, meta.retained]))
    a.topic('tick').send(2)
    await settled()

    expect(log).toEqual([
      [1, true], // replayed on subscribe
      [2, false], // live
    ])
    hub.close()
  })

  it('Quick Start, the RPC half over a hub topic', async () => {
    const hub = Hub.create<Bus>('app')
    const worker = hub.local()
    const page = hub.local()
    await Promise.all([worker.ready(), page.ready()])

    api.right(worker.topic('api')).serve({
      getUser: async ({ id }) => ({ id, name: `user-${id}` }),
      watch: async function* (_query, { signal }) {
        for (let n = 0; !signal.aborted; n++) {
          yield n
          await settled()
        }
      },
    })

    const client = api.left(page.topic('api'))
    expect(await client.getUser({ id: '1' })).toEqual({ id: '1', name: 'user-1' })
    const seen: number[] = []
    for await (const n of client.watch({ q: 'x' })) {
      seen.push(n)
      if (n > 3) break // tells the worker to stop
    }
    expect(seen).toEqual([0, 1, 2, 3, 4])
    hub.close()
  })

  it('RPC: streaming from a push source', async () => {
    const feed = pair('feed', { watch: send<{ q: string }>().stream<number>() })
    const [pa, pb] = linkPair<Frame>()
    const [upstream, source] = linkPair<number>()

    // a port, a topic or a link is already a source: `onClose(error?)` is `close`
    feed.right(pb).serve({
      watch: () => (next: (value: number) => void, close: () => void) => source.listen(next, { onClose: close }),
    })

    const it = feed.left(pa).watch({ q: 'x' })
    const first = it.next()
    await settled()
    upstream.send(1)
    expect(await first).toEqual({ value: 1, done: false })

    upstream.close() // completes the stream
    expect(await it.next()).toEqual({ value: undefined, done: true })
    pa.close()
  })

  it('RPC: a symmetric protocol, one kind of end', async () => {
    const room = symmetric('room', {
      chat: both<string>(),
      ping: both<void>().reply<number>(),
      history: both<{ q: string }>().stream<number>(),
    })
    const [pa, pb] = linkPair<Frame>()
    const a = room.connect(pa)
    const b = room.connect(pb)

    const lines: string[] = []
    b.serve({ ping: () => 7 })
    a.on.chat((line) => lines.push(line))

    b.chat('hi')
    expect(await a.ping()).toBe(7)
    await settled()
    expect(lines).toEqual(['hi'])
    pa.close()
  })

  it('RPC: meta rides along with any frame', async () => {
    const proto = pair('meta', {
      log: send<string>(),
      getUser: send<{ id: string }>().reply<{ id: string; name: string }>(),
    })
    const [pa, pb] = linkPair<Frame>()
    const l = proto.left(pa)
    const r = proto.right(pb)

    const logged: string[] = []
    let token: unknown
    r.serve({
      log: (s, ctx) => logged.push(`${s} ${String(ctx.meta['trace'])}`),
      getUser: ({ id }, ctx) => {
        token = ctx.meta['token']
        return { id, name: 'x' }
      },
    })

    const replies: unknown[] = []
    l.listen((m) => {
      if (m.re === 'ok') replies.push(m.meta)
    })

    l.log('hello', { meta: { trace: 't1' } })
    expect(await l.getUser({ id: '1' }, { meta: { token: 'k' } })).toEqual({ id: '1', name: 'x' })
    await settled()

    expect(logged).toEqual(['hello t1'])
    expect(token).toBe('k')
    expect(replies).toEqual([undefined]) // the responder attached none
    pa.close()
  })

  it('Core Concepts: Link', async () => {
    const [a, b] = linkPair<string>()
    const got: string[] = []
    b.listen((m) => got.push(m))
    a.send('hi')
    expect(got).toEqual([]) // delivered on a microtask, like a real transport
    await settled()
    expect(got).toEqual(['hi'])
    a.close()
  })

  it('RPC: both sides over any port', async () => {
    const chat = pair('chat', {
      log: send<string>(),
      push: recv<{ at: number }>(),
      ready: both<void>(),
      getUser: send<{ id: string }>().reply<{ id: string; name: string }>(),
      watch: send<{ q: string }>().stream<number>(),
    })
    const [pa, pb] = linkPair<Frame>()
    const l = chat.left(pa)
    const r = chat.right(pb)

    const logged: string[] = []
    r.serve({
      log: (s) => logged.push(s),
      getUser: async ({ id }) => ({ id, name: 'x' }),
      watch: function* ({ q }, { signal }) {
        for (const n of [1, 2, 3]) {
          if (signal.aborted) return
          yield n * q.length
        }
      },
    })

    l.log('hello')
    expect(await l.getUser({ id: '1' })).toEqual({ id: '1', name: 'x' })
    const rows: number[] = []
    for await (const row of l.watch({ q: 'ab' })) rows.push(row)
    expect(rows).toEqual([2, 4, 6])
    await settled()
    expect(logged).toEqual(['hello'])
    pa.close()
  })

  it('Replica: a writer and a reader over hub topics', async () => {
    type Rep = { counter: replica.Replica.Message<number, number> }
    const hub = Hub.create<Rep>('rep')
    const ws = hub.local()
    const rs = hub.local()
    await Promise.all([ws.ready(), rs.ready()])
    const clock = fakeClock()

    const counter = replica.define({
      initial: 0,
      apply: (state, delta: number) => state + delta,
      clock,
      jitter: 0,
    })
    const w = counter.w(ws.topic('counter'))
    const r = counter.r(rs.topic('counter'))
    const pinned = counter.r(hub.local().topic('counter'), 'stable-id')
    expect(pinned.id).toBe('stable-id')
    pinned.close()

    w.update(2)
    expect(r.status).toBe('joining')
    clock.advance(0) // the first snapshot request
    await settled()
    await settled()
    expect(r.status).toBe('live')
    expect(r.get()).toBe(2)

    const seen: number[] = []
    r.listen((state) => seen.push(state))
    w.update(3)
    await settled()
    expect(seen).toEqual([5])
    r.close()
    w.close()
    hub.close()
  })

  it('Testing: a persistent link under a fake clock', () => {
    const clock = fakeClock()
    let attempts = 0
    const link = persistent(
      () => {
        attempts++
        return linkPair<string>()[0]
      },
      { clock, jitter: 0, timeout: 100, onError: () => {} },
    )
    expect(link.state).toBe('connecting')
    clock.advance(100) // no greeting → retrying
    expect(link.state).toBe('retrying')
    expect(attempts).toBe(1)
    clock.advance(250) // first backoff → second attempt
    expect(attempts).toBe(2)
    link.close()
  })
})

// --- browser samples: type-checked, never run --------------------------------

/** page.ts — the hub, its workers, and the caller half of the protocol. */
const pageSample = async () => {
  const hub = Hub.create<Bus>('app', { retain: ['tick'] })

  hub.serve(worker('./worker-a.ts'))
  hub.serve(persistent(() => link(new Worker('./worker-b.ts'))))

  const sesh = hub.local()
  sesh.topic('tick').send(1)

  const client = api.left(sesh.topic('api'))
  await client.getUser({ id: '1' })
  for await (const n of client.watch({ q: 'x' })) {
    if (n > 3) break
  }
}

/** worker-a.ts — joins by name over the port it already holds, and responds. */
const workerSample = async () => {
  const session = Session.over<Bus>('app', link(workerSelf()))
  await session.ready()

  session.topic('tick').listen((t, meta) => console.log(t, meta.retained ? '(replayed)' : ''))
  session.topic('tick').send(2)

  api.right(session.topic('api')).serve({
    getUser: async ({ id }) => ({ id, name: `user-${id}` }),
    watch: async function* (_query, { signal }) {
      for (let n = 0; !signal.aborted; n++) {
        yield n
        await new Promise((r) => setTimeout(r, 1000))
      }
    },
  })
}

/** Peers that connect themselves: a frame, its relaying parent, and the hub. */
const handshakeSample = () => {
  const frame = app.connect(connector('app', fromWindow(parent, 'https://host.example')))
  bridge(window, new Worker('./worker.js'), { origins: ['https://frame.example'] })
  const hub = app.hub()
  hub.serve(handshake(workerSelf(), { origins: ['https://frame.example'] }))
  return frame
}

void [pageSample, workerSample, handshakeSample]

// --- adapter samples: type-checked, never run --------------------------------

/**
 * The README writes these against ambient `chrome.runtime` and node's
 * `worker_threads`. Neither is a dependency here, so the shapes are declared
 * structurally — which is also the point: nothing in `@lickle/wire/adapter`
 * knows what a chrome port is.
 */
declare namespace chrome.runtime {
  interface Event<A extends unknown[]> {
    addListener(fn: (...a: A) => void): void
    removeListener(fn: (...a: A) => void): void
  }
  interface Port {
    readonly name: string
    readonly sender?: { readonly origin?: string } | undefined
    postMessage(message: unknown): void
    disconnect(): void
    onMessage: Event<[unknown, Port]>
    onDisconnect: Event<[Port]>
  }
  const id: string
  const lastError: { message?: string } | undefined
  const onConnect: Event<[Port]>
}

const chromeLink = <T>(
  port: chrome.runtime.Port,
  runtime: typeof chrome.runtime,
  opts: { meta?: Link.Meta } = {},
): Link<T> =>
  defineLink<T>(
    (host) => {
      const onMessage = (msg: unknown) => host.deliver(msg as T)
      const onDisconnect = () => {
        void runtime.lastError
        host.shut()
      }
      port.onMessage.addListener(onMessage)
      port.onDisconnect.addListener(onDisconnect)

      return {
        send: (msg) => {
          try {
            port.postMessage(msg)
            return true
          } catch (err) {
            host.fail(err)
            host.shut()
            return false
          }
        },
        release: () => {
          port.onMessage.removeListener(onMessage)
          port.onDisconnect.removeListener(onDisconnect)
          port.disconnect()
        },
      }
    },
    { remote: port.name, meta: { ...opts.meta, sender: port.sender } },
  )

const chromeSource = <T>(runtime: typeof chrome.runtime): Link.Source<T> =>
  defineSource<T>((host) => {
    const onConnect = (port: chrome.runtime.Port) => host.offer(chromeLink<T>(port, runtime))
    runtime.onConnect.addListener(onConnect)
    return () => runtime.onConnect.removeListener(onConnect)
  })

/** ...and the same, vetted: `accept` supplies the policy, chrome only the arrival. */
const chromeAccept = <T>(runtime: typeof chrome.runtime): Link.Source<T> =>
  accept<T, chrome.runtime.Port>(
    (take) => {
      const on = (p: chrome.runtime.Port) =>
        take({ name: p.name, origin: p.sender?.origin ?? '', path: [], channel: p })
      runtime.onConnect.addListener(on)
      return () => runtime.onConnect.removeListener(on)
    },
    (offer, meta) => chromeLink<T>(offer.channel, runtime, { meta }),
    { origins: [`chrome-extension://${runtime.id}`] },
  )

/** node `worker_threads`: an EventEmitter, so it satisfies no DOM target type. */
interface ThreadPort {
  on(event: 'message', fn: (msg: any) => void): void
  off(event: 'message', fn: (msg: any) => void): void
  once(event: 'close', fn: () => void): void
  postMessage(msg: unknown): void
  close(): void
}

const threadLink = <T>(port: ThreadPort): Link<T> =>
  defineLink<T>((host) => {
    const on = (msg: T) => host.deliver(msg)
    port.on('message', on)
    port.once('close', host.shut)
    return {
      send: (msg) => (port.postMessage(msg), true),
      release: () => {
        port.off('message', on)
        port.close()
      },
    }
  })

const socketLink = <T>(url: string): Link<T> =>
  defineLink<T>(
    (host) => {
      const ws = new WebSocket(url)
      const onOpen = () => host.signal(true)
      const onMessage = (e: MessageEvent) => {
        try {
          host.deliver(JSON.parse(String(e.data)) as T)
        } catch (err) {
          host.fail(err)
        }
      }
      const onError = (e: Event) => host.fail(e)
      const onClose = () => host.shut()

      ws.addEventListener('open', onOpen)
      ws.addEventListener('message', onMessage)
      ws.addEventListener('error', onError)
      ws.addEventListener('close', onClose)

      return {
        send: (msg) => {
          if (ws.readyState !== WebSocket.OPEN) return false
          ws.send(JSON.stringify(msg))
          return true
        },
        release: () => {
          ws.removeEventListener('open', onOpen)
          ws.removeEventListener('message', onMessage)
          ws.removeEventListener('error', onError)
          ws.removeEventListener('close', onClose)
          if (ws.readyState < WebSocket.CLOSING) ws.close(1000)
        },
      }
    },
    { remote: url, meta: { url }, up: false },
  )

/** Relaying frame handshakes to a worker the page replaces. */
const swappableWorkerSample = () => {
  const wire = slot<Envelope>({ own: true }) // the page's own peer link
  let current: Worker | null = null

  const to: Browser.Sink = { postMessage: (m, t) => current?.postMessage(m, t ?? []) }
  bridge(window, to, { origins: ['https://frame.example'] }) // once, for good

  const spawn = () => {
    current = new Worker('./hub.js', { type: 'module' })
    wire.use(link(current)) // closes the worker it replaces, under `own`
  }
  return spawn
}

/** Many logical links over one real one. */
const muxSample = (socketLink: Link<Mux.Wire>) => {
  const wire = mux<Envelope>(socketLink, { side: 'a' }) // the other end is 'b'

  const session = app.session(wire.open('panel')) // a channel, which is just a Link
  const hub = app.hub()
  hub.serve(wire.incoming) // channels the far side opened, as peers
  return session
}

/** A link whose transport is swapped by hand. */
const slotSample = () => {
  const wire = slot<Envelope>({ buffer: 64 })
  const session = app.session(wire) // subscribes once, to the slot

  const off = wire.use(link(new Worker('./a.ts'))) // attach; `off` detaches
  wire.use(link(new Worker('./b.ts'))) // swap — `off` is now inert
  void wire.inner // the transport in use, or null
  return [session, off] as const
}

/** The layering: a platform module stops at `Port`, and everything above lifts. */
const layeringSample = () => {
  const w = new Worker('./worker.js')

  // DOM -> wire. Two members is all rpc and replica ever wanted: no lifecycle,
  // no heartbeat, no options to get wrong.
  void api.left(asPort<Frame>(w))

  // ...and up a tier when routing needs liveness.
  const hub = app.hub()
  hub.serve(asLink(asPort<Envelope>(w)))

  // wire -> DOM, for code that speaks postMessage. Stacks in either order.
  const dom = asTarget(hub.local().topic('api'))
  void link(dom)
  void asPort(dom)

  // ...or keep both faces on one object when liveness is still wanted.
  const both = asDuplex(asLink(asPort<Envelope>(w)))
  both.changed((up) => void up)
  both.postMessage({ hello: true })
  both.close()
}

void [
  chromeLink,
  chromeSource,
  chromeAccept,
  threadLink,
  socketLink,
  layeringSample,
  slotSample,
  muxSample,
  swappableWorkerSample,
]
