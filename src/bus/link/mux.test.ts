import { describe, expect, it, vi } from 'vitest'
import { Hub } from '../hub.ts'
import type { Envelope } from '../protocol.ts'
import { Session } from '../session.ts'
import { mux, pipe, type Mux } from './mux.ts'
import { slot } from './slot.ts'
import { pair } from './pair.ts'
import type { Link } from './index.ts'

const settled = () => new Promise<void>((r) => setTimeout(r, 0))

/** Two muxes over one in-memory pair, as the two ends of a real connection. */
const wired = <T>(opts: Partial<Mux.Options> = {}) => {
  const [left, right] = pair<Mux.Wire>('a', 'b')
  return {
    left,
    right,
    a: mux<T>(left, { side: 'a', ...opts }),
    b: mux<T>(right, { side: 'b', ...opts }),
  }
}

/** Everything a source hands over. */
const served = <T>(m: Mux<T>) => {
  const got: Link<T>[] = []
  const stop = m.incoming((one) => got.push(one))
  return { got, stop }
}

describe('mux', () => {
  it('opens a channel that surfaces on the far side and carries both ways', async () => {
    const { a, b } = wired<string>()
    const inbound = served(b)

    const near = a.open('app', { meta: { role: 'panel' } })
    await settled()

    expect(inbound.got).toHaveLength(1)
    const far = inbound.got[0] as Link<string>
    expect(far.remote).toBe('app')
    expect(far.meta['role']).toBe('panel')
    // `via` names the link it arrived over, from the receiving end's point of view.
    expect(far.meta['via']).toBe('a')

    const there = vi.fn()
    const back = vi.fn()
    far.listen(there)
    near.listen(back)

    near.send('ping')
    await settled()
    expect(there).toHaveBeenCalledWith('ping')

    far.send('pong')
    await settled()
    expect(back).toHaveBeenCalledWith('pong')
  })

  it('keeps many channels apart on one link', async () => {
    const { a, b } = wired<string>()
    const inbound = served(b)

    const one = a.open('one')
    const two = a.open('two')
    await settled()
    expect(inbound.got.map((l) => l.remote)).toEqual(['one', 'two'])

    const first = vi.fn()
    const second = vi.fn()
    ;(inbound.got[0] as Link<string>).listen(first)
    ;(inbound.got[1] as Link<string>).listen(second)

    one.send('to-one')
    two.send('to-two')
    await settled()

    expect(first).toHaveBeenCalledExactlyOnceWith('to-one')
    expect(second).toHaveBeenCalledExactlyOnceWith('to-two')
    expect(a.size).toBe(2)
    expect(b.size).toBe(2)
  })

  it('allocates from disjoint halves of the id space, in both directions', async () => {
    const { a, b } = wired<string>()
    const atA = served(a)
    const atB = served(b)

    a.open('from-a')
    b.open('from-b')
    await settled()

    expect(atB.got.map((l) => l.remote)).toEqual(['from-a'])
    expect(atA.got.map((l) => l.remote)).toEqual(['from-b'])
    // Two ends opening at once cannot collide.
    expect(a.size).toBe(2)
  })

  it('rejects an inbound open from this end of the id space', async () => {
    const onError = vi.fn()
    const [left, right] = pair<Mux.Wire>('a', 'b')
    // Both ends misconfigured as side 'a'.
    const a = mux<string>(left, { side: 'a', onError })
    const clone = mux<string>(right, { side: 'a' })
    const inbound = served(a)

    clone.open('collides')
    await settled()

    expect(inbound.got).toHaveLength(0)
    expect(onError).toHaveBeenCalledTimes(1)
    expect(String(onError.mock.calls[0]?.[0])).toMatch(/id space/)
  })

  it('closes one channel without touching the others', async () => {
    const { a, b } = wired<string>()
    const inbound = served(b)
    const one = a.open('one')
    const two = a.open('two')
    await settled()

    const ended = vi.fn()
    ;(inbound.got[0] as Link<string>).closed(ended)
    one.close()
    await settled()

    expect(ended).toHaveBeenCalledTimes(1)
    expect((inbound.got[1] as Link<string>).up).toBe(true)
    expect(two.up).toBe(true)
    expect(a.size).toBe(1)
    expect(b.size).toBe(1)
  })

  it('holds inbound channels until something serves them', async () => {
    const { a, b } = wired<string>()
    a.open('early')
    await settled()

    const inbound = served(b)
    expect(inbound.got.map((l) => l.remote)).toEqual(['early'])
  })

  it('refuses more than the backlog rather than queueing peers forever', async () => {
    const onError = vi.fn()
    const { a, b } = wired<string>({ backlog: 1, onError })
    a.open('one')
    a.open('two')
    await settled()

    expect(onError).toHaveBeenCalledTimes(1)
    expect(String(onError.mock.calls[0]?.[0])).toMatch(/backlog/)
    const inbound = served(b)
    expect(inbound.got).toHaveLength(1)
  })

  it('takes every channel down with the link, and back up again', async () => {
    const [left, right] = pair<Mux.Wire>('a', 'b')
    const a = mux<string>(left, { side: 'a' })
    const b = mux<string>(right, { side: 'b' })
    const inbound = served(b)
    const one = a.open('app')
    await settled()
    expect(one.up).toBe(true)

    const ended = vi.fn()
    one.closed(ended)
    left.close()
    await settled()

    // The wire is gone, so the channels on it are too.
    expect(one.up).toBe(false)
    expect(ended).toHaveBeenCalledTimes(1)
    expect(a.size).toBe(0)
    expect((inbound.got[0] as Link<string>).up).toBe(false)
    void b
  })

  it('closing the mux ends its channels but not the link under it', async () => {
    const { a, left } = wired<string>()
    const one = a.open('app')
    await settled()

    a.close()
    expect(one.up).toBe(false)
    expect(a.size).toBe(0)
    // The mux did not open the link, so it does not close it.
    expect(left.up).toBe(true)
  })

  it('hands back an already-closed channel once it is over', () => {
    const { a } = wired<string>()
    a.close()
    const one = a.open('late')
    expect(one.up).toBe(false)
    expect(one.send('x')).toBe(false)
  })

  it('ignores traffic that is not its own, so a link can be shared', async () => {
    const onError = vi.fn()
    const [near, far] = pair<any>('x', 'y')
    const m = mux<string>(far, { side: 'b', onError })
    const seen = served(m)
    const stray = vi.fn()
    m.incoming(stray)

    // A hub envelope and a browser control frame on the same wire.
    near.send({ $: 'app', v: 2, t: 'ready', id: 'p1' })
    near.send({ kind: 'wire/ctrl', c: 'ping', id: 1 })
    await settled()

    expect(seen.got).toHaveLength(0)
    expect(onError).not.toHaveBeenCalled()
  })
})

describe('pipe', () => {
  it('carries traffic both ways and ends both sides together', async () => {
    const [a1, a2] = pair<string>('a1', 'a2')
    const [b1, b2] = pair<string>('b1', 'b2')
    pipe(a2, b1)

    const atB = vi.fn()
    const atA = vi.fn()
    b2.listen(atB)
    a1.listen(atA)

    a1.send('there')
    await settled()
    expect(atB).toHaveBeenCalledWith('there')

    b2.send('back')
    await settled()
    expect(atA).toHaveBeenCalledWith('back')

    const ended = vi.fn()
    b2.closed(ended)
    a1.close()
    await settled()
    expect(ended).toHaveBeenCalledTimes(1)
  })

  it('detaches without closing either side', async () => {
    const [a1, a2] = pair<string>('a1', 'a2')
    const [b1, b2] = pair<string>('b1', 'b2')
    const stop = pipe(a2, b1)

    stop()
    const atB = vi.fn()
    b2.listen(atB)
    a1.send('dropped')
    await settled()

    expect(atB).not.toHaveBeenCalled()
    expect(a1.up).toBe(true)
    expect(b2.up).toBe(true)
  })
})

describe('mux + hub', () => {
  it('serves peers that connected through one link', async () => {
    type Bus = { tick: number }
    const [left, right] = pair<Mux.Wire>('client', 'server')
    const client = mux<Envelope>(left, { side: 'a' })
    const server = mux<Envelope>(right, { side: 'b' })

    const hub = Hub.create<Bus>('app')
    hub.serve(server.incoming)

    // Two independent sessions, one wire.
    const one = Session.over<Bus>('app', client.open('one'))
    const two = Session.over<Bus>('app', client.open('two'))
    const seen: number[] = []
    one.topic('tick').listen((n) => seen.push(n))
    await settled()

    expect(one.connected).toBe(true)
    expect(two.connected).toBe(true)
    expect(hub.peers).toHaveLength(2)
    expect(hub.peers.map((p) => p.name).sort()).toEqual(['one', 'two'])

    hub.publish('tick', 1)
    await settled()
    expect(seen).toEqual([1])

    // One peer leaving does not disturb the other.
    two.close()
    await settled()
    expect(hub.peers).toHaveLength(1)
    expect(one.connected).toBe(true)

    hub.close()
  })
})

describe('mux over a link that outlives its transport', () => {
  /** A "worker" behind a slot, so it can be swapped the way a real one is. */
  const rig = () => {
    const wire = slot<Mux.Wire>({ own: true })
    const near = mux<string>(wire, { side: 'a' })
    const spawn = (name: string) => {
      const [page, worker] = pair<Mux.Wire>('page', name)
      const far = mux<string>(worker, { side: 'b' })
      const arrived: Link<string>[] = []
      far.incoming((one) => arrived.push(one))
      return { page, far, arrived }
    }
    return { wire, near, spawn }
  }

  it('announces its channels again on the new transport, so they do not dead-end', async () => {
    const { wire, near, spawn } = rig()
    const first = spawn('worker-1')
    wire.use(first.page)

    const ch = near.open('frame')
    await settled()
    expect(first.arrived).toHaveLength(1)

    // Swap the worker, as a restart or a hot reload would.
    const second = spawn('worker-2')
    wire.use(second.page)
    await settled()

    // The channel is the same object to its consumer, and the new worker knows it.
    expect(second.arrived).toHaveLength(1)
    expect(second.arrived[0]?.remote).toBe('frame')
    expect(ch.up).toBe(true)

    const got = vi.fn()
    second.arrived[0]?.listen(got)
    ch.send('after-swap')
    await settled()
    expect(got).toHaveBeenCalledWith('after-swap')
  })

  it('takes channels down while nothing is bound, and back up on the next one', async () => {
    const { wire, near, spawn } = rig()
    const off = wire.use(spawn('worker-1').page)
    const ch = near.open('frame')
    const ups: boolean[] = []
    ch.changed((up) => ups.push(up))
    await settled()

    off()
    expect(ch.up).toBe(false)

    wire.use(spawn('worker-2').page)
    await settled()
    expect(ch.up).toBe(true)
    expect(ups).toEqual([false, true])
  })

  it('announces a channel opened while nothing was bound', async () => {
    const { wire, near, spawn } = rig()
    const ch = near.open('early') // no transport at all yet
    expect(ch.up).toBe(false)

    const worker = spawn('worker-1')
    wire.use(worker.page)
    await settled()

    expect(worker.arrived).toHaveLength(1)
    expect(worker.arrived[0]?.remote).toBe('early')
  })

  it('replaces the far end of a channel it already knew, rather than refusing it', async () => {
    // The same peer on both sides of a blip: it considers the channel
    // continuous, the far side cannot, so the old peer ends and a fresh one
    // takes its place.
    const wire = slot<Mux.Wire>()
    const near = mux<string>(wire, { side: 'a' })
    const [page, worker] = pair<Mux.Wire>('page', 'worker')
    const onError = vi.fn()
    const far = mux<string>(worker, { side: 'b', onError })
    const arrived: Link<string>[] = []
    far.incoming((one) => arrived.push(one))

    const off = wire.use(page)
    near.open('frame')
    await settled()
    expect(arrived).toHaveLength(1)

    const ended = vi.fn()
    arrived[0]?.closed(ended)

    // Same wire back again: the mux re-announces.
    off()
    wire.use(page)
    await settled()

    expect(ended).toHaveBeenCalledTimes(1)
    expect(arrived).toHaveLength(2)
    expect(far.size).toBe(1)
    expect(onError).not.toHaveBeenCalled()
  })

  it('carries a session across a worker swap', async () => {
    type Bus = { tick: number }
    const wire = slot<Mux.Wire>({ own: true })
    const near = mux<Envelope>(wire, { side: 'a' })
    const session = Session.over<Bus>('app', near.open('panel'))
    const seen: number[] = []
    session.topic('tick').listen((n) => seen.push(n))

    const boot = (name: string) => {
      const [page, worker] = pair<Mux.Wire>('page', name)
      const far = mux<Envelope>(worker, { side: 'b' })
      const hub = Hub.create<Bus>('app')
      hub.serve(far.incoming)
      return { page, hub }
    }

    const first = boot('worker-1')
    wire.use(first.page)
    await settled()
    expect(session.connected).toBe(true)
    first.hub.publish('tick', 1)
    await settled()
    expect(seen).toEqual([1])

    // Restart the worker. Nothing re-subscribes by hand.
    const second = boot('worker-2')
    wire.use(second.page)
    await settled()

    expect(session.connected).toBe(true)
    expect(second.hub.peers).toHaveLength(1)
    second.hub.publish('tick', 2)
    await settled()
    expect(seen).toEqual([1, 2])

    session.close()
    first.hub.close()
    second.hub.close()
  })
})
