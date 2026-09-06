import { describe, expect, it, vi } from 'vitest'
import { Hub, type Unsub } from './index.ts'
import { reader, readerWriter, writer, type Replica } from './replica/index.ts'
import { pair, send, type Frame } from './rpc/index.ts'
import { fakeClock, type FakeClock } from './bus/testing.ts'

/**
 * Real components end to end: a `Hub`, `hub.local()` sessions, and a
 * `session.topic(...)` as the `Port` under replicas and RPC. Only the replica
 * timers are virtual; delivery runs on `Link.pair`'s microtasks.
 */

type Bus = { rep: Replica.Message<number, number>; rpc: Frame }
type SetBus = { rep: Replica.Message<string[], string> }

/** Drains the microtask chains `Link.pair` delivers on. Each hop is one microtask; a macrotask flushes them all. */
const settled = () => new Promise<void>((r) => setTimeout(r, 0))
const flush = async (times = 3) => {
  for (let i = 0; i < times; i++) await settled()
}

const counter = (clock: FakeClock): Replica.Def<number, number> => ({
  initial: 0,
  apply: (s, u) => s + u,
  clock,
  jitter: 0,
})

/** Commutative across participants, so a mesh with no authority converges exactly. */
const union = (clock: FakeClock): Replica.MergeDef<string[], string> => ({
  initial: [],
  apply: (s, u) => [...new Set([...s, u])].sort(),
  merge: (mine, theirs) => [...new Set([...mine, ...theirs])].sort(),
  clock,
  jitter: 0,
})

const cluster = async (clock: FakeClock) => {
  const hub = Hub.create<Bus>('bus')
  const sw = hub.local()
  const s1 = hub.local()
  const s2 = hub.local()
  await Promise.all([sw.ready(), s1.ready(), s2.ready()])
  const w = writer({ ...counter(clock), id: 'w' }, sw.topic('rep'))
  const r1 = reader({ ...counter(clock), id: 'r1' }, s1.topic('rep'))
  const r2 = reader({ ...counter(clock), id: 'r2' }, s2.topic('rep'))
  return { hub, w, r1, r2 }
}

describe('replicas over hub topics', () => {
  it('a writer and two readers converge', async () => {
    const clock = fakeClock()
    const { hub, w, r1, r2 } = await cluster(clock)

    w.update(5)
    await flush()
    // Nothing has asked yet: the join request is clock-driven.
    expect([r1.status, r2.status]).toEqual(['joining', 'joining'])
    expect([r1.get(), r2.get()]).toEqual([0, 0])

    clock.advance(0)
    await flush()
    expect([r1.status, r2.status]).toEqual(['live', 'live'])
    expect([r1.get(), r2.get()]).toEqual([5, 5])
    expect(clock.pending).toBe(0)

    const seen = vi.fn()
    r2.listen(seen)
    w.update(3)
    await flush()
    expect([w.get(), r1.get(), r2.get()]).toEqual([8, 8, 8])
    expect(seen.mock.calls).toEqual([[8]])

    hub.close()
  })

  it('readerWriters converge in a mesh under concurrent writes, and a late joiner catches up', async () => {
    const clock = fakeClock()
    const hub = Hub.create<SetBus>('bus')
    const sa = hub.local()
    const sb = hub.local()
    const sc = hub.local()
    await Promise.all([sa.ready(), sb.ready(), sc.ready()])
    const a = readerWriter({ ...union(clock), id: 'a', join: false }, sa.topic('rep'))
    const b = readerWriter({ ...union(clock), id: 'b', join: false }, sb.topic('rep'))
    const c = readerWriter({ ...union(clock), id: 'c', join: false }, sc.topic('rep'))

    a.update('x')
    b.update('y')
    c.update('z')
    await flush()
    for (const p of [a, b, c]) expect(p.get()).toEqual(['x', 'y', 'z'])

    // No authority anywhere: the joiner is answered by whoever is live.
    const sd = hub.local()
    await sd.ready()
    const d = readerWriter({ ...union(clock), id: 'd' }, sd.topic('rep'))
    await flush()
    expect(d.status).toBe('joining')
    clock.advance(0)
    await flush()
    expect(d.status).toBe('live')
    expect(d.get()).toEqual(['x', 'y', 'z'])

    d.update('w')
    await flush()
    for (const p of [a, b, c, d]) expect(p.get()).toEqual(['w', 'x', 'y', 'z'])
    expect(clock.pending).toBe(0)

    hub.close()
  })

  it('hub.close() ends every replica cleanly', async () => {
    const clock = fakeClock()
    const { hub, w, r1, r2 } = await cluster(clock)
    const all = [w, r1, r2]
    const closes = all.map((x) => {
      const onClose = vi.fn()
      x.listen(() => {}, { onClose })
      return onClose
    })
    // Both readers still hold a join timer.
    expect(clock.pending).toBe(2)

    hub.close()

    for (const x of all) {
      expect(x.status).toBe('closed')
      expect(x.error).toBeUndefined()
    }
    for (const onClose of closes) {
      expect(onClose).toHaveBeenCalledTimes(1)
      expect(onClose).toHaveBeenCalledWith(undefined)
    }
    expect(clock.pending).toBe(0)
    await flush()
    for (const x of all) expect(x.status).toBe('closed')
  })
})

describe('rpc served per peer over a hub', () => {
  it('two clients calling concurrently each get their own answer', async () => {
    const api = pair('api', { getUser: send<{ id: string }>().reply<{ id: string; name: string }>() })
    const hub = Hub.create<Bus>('bus')
    const server = hub.local()
    await server.ready()

    // One `right` per connected peer, each over an addressed port. Both clients
    // number their calls from '1', so without the per-peer filter the server
    // side serving client A would also see B's request and answer A with it.
    const serving = new Map<string, Unsub>()
    hub.onPeersChange((peers) => {
      for (const p of peers) {
        if (p.id === server.id || serving.has(p.id)) continue
        const side = api.right(server.topic('rpc').peer(p.id))
        serving.set(
          p.id,
          side.serve({
            getUser: async ({ id }) => {
              // The first caller's answer leaves last, so a misrouted reply would settle the wrong call.
              if (id === 'alice') await settled()
              return { id, name: `user-${id}` }
            },
          }),
        )
      }
    })

    const c1 = hub.local()
    const c2 = hub.local()
    await Promise.all([c1.ready(), c2.ready()])
    expect(serving.size).toBe(2)

    const l1 = api.left(c1.topic('rpc').peer(server.id as string))
    const l2 = api.left(c2.topic('rpc').peer(server.id as string))
    const [a, b] = await Promise.all([l1.getUser({ id: 'alice' }), l2.getUser({ id: 'bob' })])

    expect(a).toEqual({ id: 'alice', name: 'user-alice' })
    expect(b).toEqual({ id: 'bob', name: 'user-bob' })

    for (const stop of serving.values()) stop()
    hub.close()
  })
})
