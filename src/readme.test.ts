/**
 * Mirrors the code samples in README.md so they cannot drift from the API.
 *
 * Runnable samples assert the documented output. Samples that need a browser
 * runtime live in `browserSamples`, which is never called but is still
 * type-checked by `tsc --noEmit`.
 */
import { describe, expect, it } from 'vitest'

import { bridge, connector, fromWindow, handshake, link, worker, type Link } from './adapter/browser.ts'
import { define, Hub, Session, pair as linkPair, persistent } from './index.ts'
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
    feed
      .right(pb)
      .serve({
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
  const session = Session.over<Bus>('app', link(self as unknown as Link.Target))
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
  hub.serve(handshake(self as unknown as Link.Events, { origins: ['https://frame.example'] }))
  return frame
}

void [pageSample, workerSample, handshakeSample]
