import { afterEach, describe, expect, it, vi } from 'vitest'

import { define, reader, readerWriter, writer, type Replica } from './index.ts'
import { fakeClock, type FakeClock } from '../bus/testing.ts'
import type { ListenOptions, Port } from '../index.ts'

// --- harness ----------------------------------------------------------------

/**
 * An in-memory broadcast network. `send` fans out synchronously to every other
 * port's listeners — never back to the sender — so a test can reason about
 * ordering without awaiting anything. `hold()` captures sends instead of
 * delivering them and `release()` lets them go, so a test can reorder or drop
 * messages in between. `log` records everything any port sent, delivered or not.
 */
const mesh = <M>() => {
  type Listener = { next: (v: M) => void; onClose: ((e?: unknown) => void) | undefined }
  type Node = { listeners: Set<Listener>; closed: { error: unknown } | null }
  const nodes: Node[] = []
  const log: M[] = []
  const held: M[] = []
  const origin = new Map<M, Node>()
  let holding = false

  const fanout = (from: Node, msg: M) => {
    for (const node of nodes) {
      if (node === from || node.closed) continue
      for (const l of [...node.listeners]) l.next(msg)
    }
  }

  return {
    log,
    /** Captured since `hold()`, in send order. Mutable: reorder or splice before `release()`. */
    held,
    hold: () => {
      holding = true
    },
    release: () => {
      holding = false
      for (const msg of held.splice(0)) {
        const from = origin.get(msg)
        if (from) fanout(from, msg)
      }
      origin.clear()
    },
    port: (): Port<M> & { fail(e: unknown): void; end(): void } => {
      const node: Node = { listeners: new Set(), closed: null }
      nodes.push(node)
      const close = (error?: unknown) => {
        if (node.closed) return
        node.closed = { error }
        const all = [...node.listeners]
        node.listeners.clear()
        for (const l of all) l.onClose?.(error)
      }
      return {
        send: (msg) => {
          if (node.closed) return
          log.push(msg)
          if (holding) {
            origin.set(msg, node)
            held.push(msg)
            return
          }
          fanout(node, msg)
        },
        listen: (next, opts: ListenOptions = {}) => {
          if (node.closed) {
            opts.onClose?.(node.closed.error)
            return () => {}
          }
          const l: Listener = { next, onClose: opts.onClose }
          node.listeners.add(l)
          return () => {
            node.listeners.delete(l)
          }
        },
        end: () => close(),
        fail: (e) => close(e),
      }
    },
  }
}

// --- fixture ----------------------------------------------------------------

type Msg = Replica.Message<number, number>
type ListMsg = Replica.Message<string[], string>

const only = <M extends { type: string }, T extends M['type']>(log: M[], type: T) =>
  log.filter((m): m is Extract<M, { type: T }> => m.type === type)

/** Every `req` sent, as its addressee — `'*'` for a broadcast. */
const asks = (log: Msg[]) => only(log, 'req').map((m) => m.to ?? '*')

const upd = (from: string, version: number, update: number): Msg => ({ type: 'update', from, version, update })
const snap = (from: string, to: string | undefined, versions: Replica.Versions, snapshot: number): Msg => ({
  type: 'snapshot',
  from,
  to,
  versions,
  snapshot,
})
const req = (from: string, to?: string): Msg => ({ type: 'req', from, to })
const bye = (from: string): Msg => ({ type: 'bye', from })

const counter = (clock: FakeClock, extra: Partial<Replica.Def<number, number>> = {}): Replica.Def<number, number> => ({
  initial: 0,
  apply: (s, u) => s + u,
  clock,
  jitter: 0,
  join: { every: 300, attempts: 3 },
  gap: { every: 300, attempts: 3 },
  ...extra,
})

const list = (
  clock: FakeClock,
  extra: Partial<Replica.MergeDef<string[], string>> = {},
): Replica.MergeDef<string[], string> => ({
  initial: [],
  apply: (s, u) => [...s, u],
  merge: (mine, theirs) => [...new Set([...mine, ...theirs])].sort(),
  clock,
  jitter: 0,
  join: { every: 300, attempts: 3 },
  gap: { every: 300, attempts: 3 },
  ...extra,
})

const setup = () => ({ clock: fakeClock(), net: mesh<Msg>() })

afterEach(() => {
  vi.restoreAllMocks()
})

// ----------------------------------------------------------------------------

describe('reader()', () => {
  it('starts joining with the initial state and joins from the writer snapshot', () => {
    const { clock, net } = setup()
    const w = writer(counter(clock, { id: 'w' }), net.port())
    w.update(5)
    const r = reader(counter(clock, { id: 'r' }), net.port())
    const statuses: Replica.Status[] = []
    r.onStatus((s) => statuses.push(s))

    expect(w.status).toBe('live')
    expect(r.status).toBe('joining')
    expect(r.get()).toBe(0)
    expect(asks(net.log)).toEqual([])

    clock.advance(0)

    expect(only(net.log, 'req')).toEqual([{ type: 'req', from: 'r', to: undefined }])
    expect(only(net.log, 'snapshot')).toEqual([
      { type: 'snapshot', from: 'w', to: 'r', versions: { w: 1 }, snapshot: 5 },
    ])
    expect(r.status).toBe('live')
    expect(r.get()).toBe(5)
    expect(statuses).toEqual(['live'])
    expect(clock.pending).toBe(0)
  })

  it('applies updates in order and notifies listeners, replaying nothing on subscribe', () => {
    const { clock, net } = setup()
    const w = writer(counter(clock, { id: 'w' }), net.port())
    const r = reader(counter(clock, { id: 'r' }), net.port())
    clock.advance(0)

    const seen = vi.fn()
    const mine = vi.fn()
    r.listen(seen)
    w.listen(mine)
    expect(seen).not.toHaveBeenCalled()

    w.update(1)
    w.update(2)

    expect(mine.mock.calls).toEqual([[1], [3]])
    expect(seen.mock.calls).toEqual([[1], [3]])
    expect(r.get()).toBe(3)
    expect(only(net.log, 'update')).toEqual([
      { type: 'update', from: 'w', version: 1, update: 1 },
      { type: 'update', from: 'w', version: 2, update: 2 },
    ])
  })

  it('gives up joining after the configured attempts and goes live from initial, replaying the buffer', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r' }), net.port())
    const statuses: Replica.Status[] = []
    r.onStatus((s) => statuses.push(s))
    const seen = vi.fn()
    r.listen(seen)

    ghost.send(upd('w', 1, 7))
    expect(r.get()).toBe(0)

    clock.advance(0)
    expect(asks(net.log)).toEqual(['*'])
    clock.advance(300)
    expect(asks(net.log)).toEqual(['*', '*'])
    clock.advance(300)
    expect(asks(net.log)).toEqual(['*', '*', '*'])
    expect(r.status).toBe('joining')
    expect(seen).not.toHaveBeenCalled()

    clock.advance(300)
    expect(asks(net.log)).toHaveLength(3)
    expect(r.status).toBe('live')
    expect(statuses).toEqual(['live'])
    expect(r.get()).toBe(7)
    expect(seen.mock.calls).toEqual([[7]])
    expect(clock.pending).toBe(0)
  })

  it('is live from initial at construction with join: false, and still chases gaps', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r', join: false }), net.port())

    expect(r.status).toBe('live')
    expect(r.get()).toBe(0)
    expect(net.log).toEqual([])
    expect(clock.pending).toBe(0)

    ghost.send(upd('w', 2, 2))
    clock.advance(300)
    expect(asks(net.log)).toEqual(['w'])
  })

  it('sheds the oldest buffered update past join.limit and recovers the hole as a gap', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r', join: { every: 300, attempts: 3, limit: 2 } }), net.port())

    ghost.send(upd('w', 1, 1))
    ghost.send(upd('w', 2, 2))
    ghost.send(upd('w', 3, 3))

    // Three unanswered asks, then live at 900 with v2 and v3 held behind the shed v1.
    clock.advance(900)
    expect(asks(net.log)).toEqual(['*', '*', '*'])
    expect(r.status).toBe('live')
    expect(r.get()).toBe(0)
    expect(clock.pending).toBe(1)

    clock.advance(300)
    expect(asks(net.log)).toEqual(['*', '*', '*', 'w'])

    ghost.send(snap('w', 'r', { w: 3 }, 6))
    expect(r.get()).toBe(6)
    expect(clock.pending).toBe(0)
  })

  it('resolves a reordered update within the grace period without a request', () => {
    const { clock, net } = setup()
    const w = writer(counter(clock, { id: 'w' }), net.port())
    const r = reader(counter(clock, { id: 'r', join: false }), net.port())
    const seen = vi.fn()
    r.listen(seen)

    net.hold()
    w.update(1)
    w.update(2)
    net.held.reverse()
    net.release()

    // One emission: v2 was held until v1 landed, then both applied together.
    expect(seen.mock.calls).toEqual([[3]])
    expect(r.get()).toBe(3)
    clock.advance(1000)
    expect(asks(net.log)).toEqual([])
    expect(clock.pending).toBe(0)
  })

  it('chases a lost update: two directed requests, then a broadcast, then rests', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r', join: false }), net.port())

    ghost.send(upd('w', 1, 1))
    ghost.send(upd('w', 3, 3))
    expect(r.get()).toBe(1)
    expect(asks(net.log)).toEqual([])

    clock.advance(300)
    expect(asks(net.log)).toEqual(['w'])
    clock.advance(300)
    expect(asks(net.log)).toEqual(['w', 'w'])
    clock.advance(300)
    expect(asks(net.log)).toEqual(['w', 'w', '*'])
    clock.advance(300)
    expect(asks(net.log)).toEqual(['w', 'w', '*'])
    expect(clock.pending).toBe(0)
    expect(r.get()).toBe(1)
  })

  it('starts a fresh chase when the peer speaks again after an exhausted one', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    reader(counter(clock, { id: 'r', join: false }), net.port())

    ghost.send(upd('w', 1, 1))
    ghost.send(upd('w', 3, 3))
    clock.advance(1200)
    expect(asks(net.log)).toEqual(['w', 'w', '*'])
    expect(clock.pending).toBe(0)

    ghost.send(upd('w', 4, 4))
    expect(clock.pending).toBe(1)
    clock.advance(300)
    expect(asks(net.log)).toEqual(['w', 'w', '*', 'w'])
  })

  it('runs one chase per peer however many updates arrive ahead of the gap', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    reader(counter(clock, { id: 'r', join: false }), net.port())

    ghost.send(upd('w', 3, 3))
    ghost.send(upd('w', 4, 4))
    ghost.send(upd('w', 5, 5))
    ghost.send(upd('w', 6, 6))
    expect(clock.pending).toBe(1)

    clock.advance(300)
    expect(asks(net.log)).toEqual(['w'])
    ghost.send(upd('w', 7, 7))
    expect(clock.pending).toBe(1)
    clock.advance(300)
    expect(asks(net.log)).toEqual(['w', 'w'])
  })

  it('repositions the peer from a snapshot answering the chase and drains what was held', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r', join: false }), net.port())
    const seen = vi.fn()

    ghost.send(upd('w', 1, 1))
    ghost.send(upd('w', 3, 3))
    ghost.send(upd('w', 4, 4))
    r.listen(seen)
    clock.advance(300)
    expect(asks(net.log)).toEqual(['w'])

    ghost.send(snap('w', 'r', { w: 2 }, 100))
    expect(r.get()).toBe(107)
    expect(seen.mock.calls).toEqual([[100], [107]])
    expect(clock.pending).toBe(0)
  })

  it('evicts the furthest-ahead held update past gap.limit, keeping those nearest the gap', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r', join: false, gap: { every: 300, attempts: 3, limit: 3 } }), net.port())

    ghost.send(upd('w', 3, 3))
    ghost.send(upd('w', 4, 4))
    ghost.send(upd('w', 5, 5))
    ghost.send(upd('w', 6, 6)) // full, and further than anything held: dropped
    ghost.send(upd('w', 2, 2)) // evicts 5
    ghost.send(upd('w', 1, 1)) // evicts 4, then drains 1..3

    expect(r.get()).toBe(6)
    expect(clock.pending).toBe(0)

    // What was evicted applies when resent.
    ghost.send(upd('w', 4, 4))
    ghost.send(upd('w', 5, 5))
    ghost.send(upd('w', 6, 6))
    expect(r.get()).toBe(21)
  })

  it('rewinds and re-chases a third party when an adopted snapshot is behind our position on it', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r', join: false }), net.port())
    const seen = vi.fn()
    r.listen(seen)

    ghost.send(upd('w', 1, 1))
    ghost.send(upd('w', 2, 2))
    expect(r.get()).toBe(3)

    // x is ahead of us, so we chase it; its answer only knows w through v1.
    ghost.send(upd('x', 2, 10))
    ghost.send(snap('x', 'r', { x: 2, w: 1 }, 11))
    expect(r.get()).toBe(11)
    expect(clock.pending).toBe(1)

    clock.advance(300)
    expect(asks(net.log)).toEqual(['w'])

    ghost.send(upd('w', 2, 2))
    expect(r.get()).toBe(13)
    expect(seen.mock.calls).toEqual([[1], [3], [11], [13]])
    expect(clock.pending).toBe(0)
  })

  it('ignores traffic from other definitions and its own echo', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const r = reader(counter(clock, { id: 'r', name: 'a' }), net.port())

    clock.advance(0)
    expect(only(net.log, 'req')).toEqual([{ type: 'req', name: 'a', from: 'r', to: undefined }])

    ghost.send({ type: 'snapshot', name: 'b', from: 'w', to: 'r', versions: { w: 1 }, snapshot: 99 })
    expect(r.status).toBe('joining')
    ghost.send({ type: 'snapshot', name: 'a', from: 'w', to: 'r', versions: { w: 1 }, snapshot: 5 })
    expect(r.status).toBe('live')
    expect(r.get()).toBe(5)

    ghost.send({ type: 'update', from: 'w', version: 2, update: 100 }) // unnamed
    ghost.send({ type: 'update', name: 'a', from: 'r', version: 1, update: 100 }) // our own id
    expect(r.get()).toBe(5)

    ghost.send({ type: 'update', name: 'a', from: 'w', version: 2, update: 1 })
    expect(r.get()).toBe(6)
  })

  it('close does not announce a fresh reader, which no peer tracks', () => {
    const { clock, net } = setup()
    const r = reader(counter(clock, { id: 'r' }), net.port())
    const onClose = vi.fn()
    const statuses: Replica.Status[] = []
    r.listen(() => {}, { onClose })
    r.onStatus((s) => statuses.push(s))
    expect(clock.pending).toBe(1)

    r.close()

    expect(only(net.log, 'bye')).toEqual([])
    expect(clock.pending).toBe(0)
    expect(r.status).toBe('closed')
    expect(r.error).toBeUndefined()
    expect(statuses).toEqual(['closed'])
    expect(onClose).toHaveBeenCalledTimes(1)
    expect(onClose).toHaveBeenCalledWith(undefined)
  })

  it('ends cleanly when the port closes and with the error when it fails', () => {
    const { clock, net } = setup()

    const p1 = net.port()
    const r1 = reader(counter(clock, { id: 'r1' }), p1)
    const close1 = vi.fn()
    r1.listen(() => {}, { onClose: close1 })
    p1.end()
    expect(r1.status).toBe('closed')
    expect(r1.error).toBeUndefined()
    expect(close1).toHaveBeenCalledTimes(1)
    expect(close1).toHaveBeenCalledWith(undefined)

    const p2 = net.port()
    const r2 = reader(counter(clock, { id: 'r2' }), p2)
    const close2 = vi.fn()
    r2.listen(() => {}, { onClose: close2 })
    const boom = new Error('port died')
    p2.fail(boom)
    expect(r2.status).toBe('closed')
    expect(r2.error).toBe(boom)
    expect(close2).toHaveBeenCalledTimes(1)
    expect(close2).toHaveBeenCalledWith(boom)

    expect(only(net.log, 'bye')).toEqual([])
    expect(clock.pending).toBe(0)
  })

  it('closes with the error when the reducer throws on remote input, keeping the last good state', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const boom = new Error('bad update')
    const r = reader(
      counter(clock, {
        id: 'r',
        join: false,
        apply: (s, u) => {
          if (u === 13) throw boom
          return s + u
        },
      }),
      net.port(),
    )
    const onClose = vi.fn()
    const statuses: Replica.Status[] = []
    r.listen(() => {}, { onClose })
    r.onStatus((s) => statuses.push(s))

    ghost.send(upd('w', 1, 1))
    ghost.send(upd('w', 2, 13))

    expect(r.status).toBe('closed')
    expect(r.error).toBe(boom)
    expect(r.get()).toBe(1)
    expect(statuses).toEqual(['closed'])
    expect(onClose).toHaveBeenCalledTimes(1)
    expect(onClose).toHaveBeenCalledWith(boom)
    expect(only(net.log, 'bye')).toEqual([])

    // Late subscribers hear the failure at once; nothing further is applied.
    const late = vi.fn()
    r.listen(() => {}, { onClose: late })
    expect(late).toHaveBeenCalledWith(boom)
    ghost.send(upd('w', 3, 1))
    expect(r.get()).toBe(1)
  })
})

describe('writer()', () => {
  it('is live at construction and answers a broadcast request at once, jitter or not', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const w = writer(counter(clock, { id: 'w', jitter: 100 }), net.port())
    expect(w.status).toBe('live')
    expect(w.get()).toBe(0)

    ghost.send(req('j'))
    expect(only(net.log, 'snapshot')).toEqual([
      { type: 'snapshot', from: 'w', to: 'j', versions: { w: 0 }, snapshot: 0 },
    ])
    expect(clock.pending).toBe(0)
  })

  it('never adopts remote state', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const w = writer(counter(clock, { id: 'w' }), net.port())
    const seen = vi.fn()
    w.listen(seen)

    ghost.send(upd('x', 1, 5))
    ghost.send(snap('x', 'w', { x: 1 }, 99))
    ghost.send(snap('x', undefined, { x: 1 }, 99))
    expect(w.get()).toBe(0)
    expect(seen).not.toHaveBeenCalled()

    // Nor does it track x: its own snapshot carries only its own position.
    ghost.send(req('j', 'w'))
    const answered = only(net.log, 'snapshot').filter((m) => m.from === 'w')
    expect(answered.map((m) => m.versions)).toEqual([{ w: 0 }])
  })

  it('propagates a reducer throw on a local update without burning a version', () => {
    const { clock, net } = setup()
    const boom = new Error('bad update')
    const w = writer(
      counter(clock, {
        id: 'w',
        apply: (s, u) => {
          if (u === 13) throw boom
          return s + u
        },
      }),
      net.port(),
    )

    expect(() => w.update(13)).toThrow(boom)
    expect(w.status).toBe('live')
    expect(w.error).toBeUndefined()
    expect(w.get()).toBe(0)
    expect(net.log).toEqual([])

    w.update(1)
    expect(only(net.log, 'update')).toEqual([{ type: 'update', from: 'w', version: 1, update: 1 }])
    expect(w.get()).toBe(1)
  })

  it('close announces bye, clears timers and ends subscribers cleanly, once', () => {
    const { clock, net } = setup()
    const w = writer(counter(clock, { id: 'w' }), net.port())
    const onClose = vi.fn()
    const statuses: Replica.Status[] = []
    w.listen(() => {}, { onClose })
    w.onStatus((s) => statuses.push(s))

    w.close()
    w.close()

    expect(only(net.log, 'bye')).toEqual([{ type: 'bye', from: 'w' }])
    expect(w.status).toBe('closed')
    expect(w.error).toBeUndefined()
    expect(statuses).toEqual(['closed'])
    expect(onClose).toHaveBeenCalledTimes(1)
    expect(onClose).toHaveBeenCalledWith(undefined)
    expect(clock.pending).toBe(0)
  })

  it('is inert after close: update sends nothing and listen reports closed synchronously', () => {
    const { clock, net } = setup()
    const w = writer(counter(clock, { id: 'w' }), net.port())
    w.update(1)
    w.close()
    const sent = net.log.length

    w.update(2)
    expect(net.log).toHaveLength(sent)
    expect(w.get()).toBe(1)

    const fn = vi.fn()
    const closeA = vi.fn()
    const closeB = vi.fn()
    w.listen(fn, { onClose: closeA })
    w.onStatus(fn, { onClose: closeB })
    expect(closeA).toHaveBeenCalledWith(undefined)
    expect(closeB).toHaveBeenCalledWith(undefined)
    expect(fn).not.toHaveBeenCalled()
  })
})

describe('readerWriter()', () => {
  it('ends the join early on a local write, landing buffered remote updates first', () => {
    const clock = fakeClock()
    const net = mesh<ListMsg>()
    const ghost = net.port()
    const rw = readerWriter(list(clock, { id: 'rw' }), net.port())

    ghost.send({ type: 'update', from: 'w', version: 1, update: 'a' })
    expect(rw.status).toBe('joining')
    expect(rw.get()).toEqual([])

    rw.update('b')

    expect(rw.status).toBe('live')
    expect(rw.get()).toEqual(['a', 'b'])
    const sent = only(net.log, 'update').filter((m) => m.from === 'rw')
    expect(sent).toEqual([{ type: 'update', from: 'rw', version: 1, update: 'b' }])
    expect(clock.pending).toBe(0)
  })

  it('reconciles an adopted snapshot through merge(mine, theirs)', () => {
    const clock = fakeClock()
    const net = mesh<ListMsg>()
    const ghost = net.port()
    const merge = vi.fn((mine: string[], theirs: string[]) => [...new Set([...mine, ...theirs])].sort())
    const rw = readerWriter(list(clock, { id: 'rw', join: false, merge }), net.port())

    rw.update('a')
    // x is ahead of us: chase it, and take the snapshot it answers with.
    ghost.send({ type: 'update', from: 'x', version: 2, update: 'q' })
    ghost.send({ type: 'snapshot', from: 'x', to: 'rw', versions: { x: 2, rw: 1 }, snapshot: ['b', 'q'] })

    expect(merge).toHaveBeenCalledTimes(1)
    expect(merge).toHaveBeenCalledWith(['a'], ['b', 'q'])
    expect(rw.get()).toEqual(['a', 'b', 'q'])
    expect(clock.pending).toBe(0)
  })

  it('answers a broadcast request after a jittered delay, once per requester', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const { clock, net } = setup()
    const ghost = net.port()
    const merge = (_mine: number, theirs: number) => theirs
    readerWriter({ ...counter(clock, { id: 'a', join: false, jitter: 100 }), merge }, net.port())

    ghost.send(req('j'))
    ghost.send(req('j'))
    expect(clock.pending).toBe(1)
    expect(only(net.log, 'snapshot')).toEqual([])

    clock.advance(49)
    expect(only(net.log, 'snapshot')).toEqual([])
    clock.advance(1)
    expect(only(net.log, 'snapshot')).toEqual([
      { type: 'snapshot', from: 'a', to: 'j', versions: { a: 0 }, snapshot: 0 },
    ])
    expect(clock.pending).toBe(0)
  })

  it('stands down when another peer answers the requester first', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const { clock, net } = setup()
    const ghost = net.port()
    const merge = (_mine: number, theirs: number) => theirs
    readerWriter({ ...counter(clock, { id: 'a', join: false, jitter: 100 }), merge }, net.port())
    const b = readerWriter({ ...counter(clock, { id: 'b', join: false, jitter: 100 }), merge }, net.port())

    ghost.send(req('j'))
    expect(clock.pending).toBe(2)

    clock.advance(50)
    const answers = only(net.log, 'snapshot')
    expect(answers).toHaveLength(1)
    expect(answers[0]?.from).toBe('a')
    expect(answers[0]?.to).toBe('j')
    expect(clock.pending).toBe(0)
    // b overheard the answer but had not asked for it.
    expect(b.get()).toBe(0)
  })

  it('answers a directed request without delay, and only when it is the addressee', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const merge = (_mine: number, theirs: number) => theirs
    readerWriter({ ...counter(clock, { id: 'a', join: false, jitter: 100 }), merge }, net.port())

    ghost.send(req('j', 'a'))
    expect(only(net.log, 'snapshot')).toEqual([
      { type: 'snapshot', from: 'a', to: 'j', versions: { a: 0 }, snapshot: 0 },
    ])
    expect(clock.pending).toBe(0)

    ghost.send(req('j', 'b'))
    expect(only(net.log, 'snapshot')).toHaveLength(1)
  })

  it('does not answer requests while still joining', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const merge = (_mine: number, theirs: number) => theirs
    const rw = readerWriter({ ...counter(clock, { id: 'a' }), merge }, net.port())

    ghost.send(req('j', 'a'))
    ghost.send(req('j'))
    expect(rw.status).toBe('joining')
    expect(only(net.log, 'snapshot')).toEqual([])
    expect(clock.pending).toBe(1)
  })

  it('close announces bye even before its first write, since it may already be a peer', () => {
    const { clock, net } = setup()
    const merge = (_mine: number, theirs: number) => theirs
    const rw = readerWriter({ ...counter(clock, { id: 'a', join: false }), merge }, net.port())

    rw.close()

    expect(only(net.log, 'bye')).toEqual([{ type: 'bye', from: 'a' }])
    expect(rw.status).toBe('closed')
    expect(rw.error).toBeUndefined()
    expect(clock.pending).toBe(0)
  })

  it('drops a peer on bye and cancels its chase', () => {
    const { clock, net } = setup()
    const ghost = net.port()
    const merge = (_mine: number, theirs: number) => theirs
    const rw = readerWriter({ ...counter(clock, { id: 'a', join: false }), merge }, net.port())

    ghost.send(upd('w', 1, 1))
    ghost.send(upd('w', 3, 3))
    expect(rw.get()).toBe(1)
    expect(clock.pending).toBe(1)

    ghost.send(bye('w'))
    expect(clock.pending).toBe(0)

    // Forgotten: our snapshot no longer carries w, and a fresh v1 from it applies again.
    ghost.send(req('j', 'a'))
    expect(only(net.log, 'snapshot').map((m) => m.versions)).toEqual([{ a: 0 }])
    ghost.send(upd('w', 1, 1))
    expect(rw.get()).toBe(2)
  })
})

describe('define()', () => {
  it('binds the definition to r and w, generating an id when none is given', () => {
    const { clock, net } = setup()
    const d = define<number, number>({ initial: 0, apply: (s, u) => s + u, clock, jitter: 0, join: false })
    const w = d.w(net.port(), 'w')
    const r = d.r(net.port())

    expect(w.type).toBe('W')
    expect(w.id).toBe('w')
    expect(r.type).toBe('R')
    expect(typeof r.id).toBe('string')
    expect(r.id).not.toBe('')
    expect(r.name).toBeUndefined()

    w.update(2)
    expect(r.get()).toBe(2)

    // @ts-expect-error `rw` is only offered when the definition supplies a `merge`
    void d.rw
  })

  it('offers rw when the definition can merge, and stamps the name on every replica', () => {
    const clock = fakeClock()
    const net = mesh<ListMsg>()
    const d = define<string[], string>({
      name: 'tags',
      initial: [],
      apply: (s, u) => [...s, u],
      merge: (mine, theirs) => [...new Set([...mine, ...theirs])].sort(),
      clock,
      jitter: 0,
      join: false,
    })
    const rw = d.rw(net.port(), 'x')
    const r = d.r(net.port(), 'y')

    expect(rw.type).toBe('RW')
    expect(rw.name).toBe('tags')
    expect(r.name).toBe('tags')
    expect(only(net.log, 'req')).toEqual([])

    rw.update('a')
    expect(r.get()).toEqual(['a'])
    expect(only(net.log, 'update')).toEqual([{ type: 'update', name: 'tags', from: 'x', version: 1, update: 'a' }])
  })
})
