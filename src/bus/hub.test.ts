import { describe, expect, it, vi } from 'vitest'
import { pair, type ILink } from './link/index.ts'
import { noop } from '../core/internal.ts'
import * as Protocol from './protocol.ts'
import { Hub } from './hub.ts'

type Bus = { a: number; b: string; x: unknown }

/** Drains the microtask chains `Link.pair` delivers on. */
const settled = () => new Promise<void>((r) => setTimeout(r, 0))

/** A raw peer: one end of a pair, the other accepted by the hub. */
const raw = (hub: Hub<Bus>, name = 'peer', meta: ILink.Meta = {}) => {
  const [mine, theirs] = pair<Protocol.Envelope>(name, hub.name, { metaA: meta })
  const sent: Protocol.Envelope[] = []
  mine.listen((m) => {
    sent.push(m)
  })
  const off = hub.serve(theirs)
  return {
    link: mine,
    /** Every envelope the hub has sent this peer. */
    sent,
    /** The teardown `serve` returned. */
    off,
    send: (f: Protocol.Frame) => mine.send(Protocol.seal(hub.name, f)),
  }
}

type DataFrame = Extract<Protocol.Frame, { t: 'data' }>

const ready = (id: string) => Protocol.seal('h', { t: 'ready', id })
const data = (f: Omit<DataFrame, 't'>) => Protocol.seal('h', { t: 'data', ...f })
const sub = (c: string): Protocol.Frame => ({ t: 'sub', c })

describe('Hub', () => {
  it('greets an accepted peer with ready and lists it', async () => {
    const hub = Hub.create<Bus>('h')
    const p = raw(hub, 'tab', { tab: 1 })
    await settled()

    expect(p.sent).toEqual([{ t: 'ready', id: 'h#1', $: 'h', v: Protocol.VERSION }])
    expect(hub.peers).toHaveLength(1)
    expect(hub.peers[0]).toMatchObject({ id: 'h#1', name: 'tab', meta: { tab: 1 }, rev: 0 })
    expect(hub.peers[0]?.channels.size).toBe(0)
    expect(hub.peer('h#1')).toBe(hub.peers[0])
    expect(hub.peer('nope')).toBeNull()
  })

  it('fans a publish out to every subscriber except the sender, stamped with from', async () => {
    const hub = Hub.create<Bus>('h')
    const [p1, p2, p3, p4] = [raw(hub), raw(hub), raw(hub), raw(hub)]
    p1.send(sub('a'))
    p2.send(sub('a'))
    p3.send(sub('a'))
    p4.send(sub('b'))
    await settled()

    p1.send({ t: 'data', c: 'a', d: 1 })
    await settled()

    expect(p1.sent).toEqual([ready('h#1')])
    expect(p2.sent).toEqual([ready('h#2'), data({ c: 'a', d: 1, from: 'h#1' })])
    expect(p3.sent).toEqual([ready('h#3'), data({ c: 'a', d: 1, from: 'h#1' })])
    expect(p4.sent).toEqual([ready('h#4')])
  })

  it('delivers an addressed frame to one peer, bypassing subscriptions', async () => {
    // The target may not subscribe, so the hub holds no channel for it; a local
    // listener is still there to receive what is addressed to it.
    const hub = Hub.create<Bus>('h', { policy: { canSubscribe: (peer) => peer.name !== 'h:local' } })
    const target = hub.local()
    const fn = vi.fn()
    target.topic('a').listen(fn)
    await target.ready()
    await settled()
    const id = target.id as string
    expect(hub.peer(id)?.channels.has('a')).toBe(false)

    const sender = raw(hub)
    await settled()
    sender.send({ t: 'data', c: 'a', d: 5, to: id })
    sender.send({ t: 'data', c: 'a', d: 6 })
    await settled()

    expect(fn).toHaveBeenCalledTimes(1)
    expect(fn).toHaveBeenCalledWith(5, { channel: 'a', from: 'h#2', retained: false })
  })

  it('replays retained values to new subscribers flagged retained; live traffic is not', async () => {
    const hub = Hub.create<Bus>('h', { retain: ['a'] })
    const pub = hub.local()
    const subber = hub.local()
    await Promise.all([pub.ready(), subber.ready()])
    pub.topic('a').send(1)
    pub.topic('b').send('not retained')
    await settled()

    const onA = vi.fn()
    const onB = vi.fn()
    subber.topic('a').listen(onA)
    subber.topic('b').listen(onB)
    await settled()
    expect(onA).toHaveBeenCalledTimes(1)
    expect(onA).toHaveBeenCalledWith(1, { channel: 'a', from: null, retained: true })
    expect(onB).not.toHaveBeenCalled()

    pub.topic('a').send(2)
    await settled()
    expect(onA).toHaveBeenCalledTimes(2)
    expect(onA).toHaveBeenLastCalledWith(2, { channel: 'a', from: 'h#1', retained: false })
  })

  it('publish() fans out or targets one peer, honours a retain override, and clearRetained forgets', async () => {
    const hub = Hub.create<Bus>('h', { retain: (c) => c === 'b' })
    const [p1, p2] = [raw(hub), raw(hub)]
    p1.send(sub('a'))
    p2.send(sub('a'))
    await settled()

    hub.publish('a', 1)
    hub.publish('a', 2, { to: 'h#2' })
    hub.publish('a', 3, { to: 'nobody' })
    await settled()
    expect(p1.sent).toEqual([ready('h#1'), data({ c: 'a', d: 1 })])
    expect(p2.sent).toEqual([ready('h#2'), data({ c: 'a', d: 1 }), data({ c: 'a', d: 2 })])

    hub.publish('x', 'kept', { retain: true })
    hub.publish('b', 'skipped', { retain: false })
    const p3 = raw(hub)
    p3.send(sub('x'))
    p3.send(sub('b'))
    await settled()
    expect(p3.sent).toEqual([ready('h#3'), data({ c: 'x', d: 'kept', r: true })])

    hub.clearRetained('x')
    hub.publish('b', 'by the rule')
    const p4 = raw(hub)
    p4.send(sub('x'))
    p4.send(sub('b'))
    await settled()
    expect(p4.sent).toEqual([ready('h#4'), data({ c: 'b', d: 'by the rule', r: true })])

    hub.clearRetained()
    const p5 = raw(hub)
    p5.send(sub('b'))
    await settled()
    expect(p5.sent).toEqual([ready('h#5')])
  })

  it('closes the link and never greets when canAccept returns false', async () => {
    const canAccept = vi.fn(() => false)
    const hub = Hub.create<Bus>('h', { policy: { canAccept } })
    const p = raw(hub)
    expect(p.link.up).toBe(false)
    await settled()
    expect(p.sent).toEqual([])
    expect(hub.peers).toEqual([])
    expect(canAccept).toHaveBeenCalledWith(expect.objectContaining({ id: 'h#1', name: 'peer' }))
  })

  it('holds frames and the greeting while an async canAccept settles', async () => {
    let admit!: (ok: boolean) => void
    const hub = Hub.create<Bus>('h', { policy: { canAccept: () => new Promise<boolean>((r) => (admit = r)) } })
    const onPeersChange = vi.fn()
    hub.onPeersChange(onPeersChange)
    const p = raw(hub)
    p.send(sub('a'))
    await settled()
    expect(p.sent).toEqual([])
    expect(onPeersChange).not.toHaveBeenCalled()

    admit(true)
    await settled()
    expect(p.sent).toEqual([ready('h#1')])
    expect(hub.peer('h#1')?.channels.has('a')).toBe(true)
    // Admitted, then the held sub applied.
    expect(onPeersChange).toHaveBeenCalledTimes(2)
  })

  it('keeps pending peers invisible in peers and peer()', async () => {
    const hub = Hub.create<Bus>('h', { policy: { canAccept: () => new Promise<boolean>(noop) } })
    const p = raw(hub)
    p.send(sub('a'))
    await settled()
    expect(hub.peers).toEqual([])
    expect(hub.peer('h#1')).toBeNull()
    expect(p.link.up).toBe(true)
  })

  it('refuses and reports when canAccept rejects or throws', async () => {
    const onError = vi.fn()
    const rejecting = Hub.create<Bus>('h', {
      onError,
      policy: { canAccept: () => Promise.reject(new Error('denied')) },
    })
    const p = raw(rejecting)
    await settled()
    expect(p.link.up).toBe(false)
    expect(p.sent).toEqual([])
    expect(rejecting.peers).toEqual([])
    expect(onError).toHaveBeenCalledWith(new Error('denied'))

    const throwing = Hub.create<Bus>('h', {
      onError,
      policy: {
        canAccept: () => {
          throw new Error('exploded')
        },
      },
    })
    const q = raw(throwing)
    expect(q.link.up).toBe(false)
    expect(onError).toHaveBeenLastCalledWith(new Error('exploded'))
    await settled()
    expect(q.sent).toEqual([])
    expect(throwing.peers).toEqual([])
  })

  it('drops a sub the policy refuses', async () => {
    const canSubscribe = vi.fn((_peer: Hub.Peer<Bus>, c: string) => c !== 'b')
    const hub = Hub.create<Bus>('h', { policy: { canSubscribe } })
    const [p1, p2] = [raw(hub), raw(hub)]
    p1.send(sub('a'))
    p1.send(sub('b'))
    await settled()
    expect(hub.peer('h#1')?.channels).toEqual(new Set(['a']))
    expect(canSubscribe).toHaveBeenCalledWith(expect.objectContaining({ id: 'h#1' }), 'b')

    p2.send({ t: 'data', c: 'b', d: 'unheard' })
    p2.send({ t: 'data', c: 'a', d: 1 })
    await settled()
    expect(p1.sent).toEqual([ready('h#1'), data({ c: 'a', d: 1, from: 'h#2' })])
  })

  it('swallows a publish the policy refuses', async () => {
    const canPublish = vi.fn(() => false)
    const hub = Hub.create<Bus>('h', { retain: ['a'], policy: { canPublish } })
    const [p1, p2] = [raw(hub), raw(hub)]
    p2.send(sub('a'))
    await settled()
    p1.send({ t: 'data', c: 'a', d: 1 })
    await settled()

    expect(canPublish).toHaveBeenCalledWith(expect.objectContaining({ id: 'h#1' }), 'a', 1)
    expect(p2.sent).toEqual([ready('h#2')])

    // Nothing was retained either.
    const p3 = raw(hub)
    p3.send(sub('a'))
    await settled()
    expect(p3.sent).toEqual([ready('h#3')])
  })

  it('blocks addressed delivery the policy refuses', async () => {
    const canAddress = vi.fn(() => false)
    const hub = Hub.create<Bus>('h', { policy: { canAddress } })
    const [p1, p2] = [raw(hub), raw(hub)]
    await settled()
    p1.send({ t: 'data', c: 'a', d: 1, to: 'h#2' })
    await settled()

    expect(canAddress).toHaveBeenCalledWith(
      expect.objectContaining({ id: 'h#1' }),
      expect.objectContaining({ id: 'h#2' }),
      'a',
    )
    expect(p2.sent).toEqual([ready('h#2')])
  })

  it('reads a throwing policy hook as a refusal and reports it', async () => {
    const onError = vi.fn()
    const hub = Hub.create<Bus>('h', {
      onError,
      policy: {
        canSubscribe: (_peer, c) => {
          if (c === 'b') throw new Error('sub!')
          return true
        },
        canPublish: () => {
          throw new Error('pub!')
        },
      },
    })
    const [p1, p2] = [raw(hub), raw(hub)]
    p2.send(sub('a'))
    p2.send(sub('b'))
    await settled()
    expect(hub.peer('h#2')?.channels).toEqual(new Set(['a']))

    p1.send({ t: 'data', c: 'a', d: 1 })
    await settled()
    expect(p2.sent).toEqual([ready('h#2')])
    expect(onError.mock.calls.map((c) => (c[0] as Error).message)).toEqual(['sub!', 'pub!'])
  })

  it('keeps peers identity until a change, then mints a new array and a new peer with rev + 1', async () => {
    const hub = Hub.create<Bus>('h')
    const onPeersChange = vi.fn()
    hub.onPeersChange(onPeersChange)
    const p = raw(hub)
    expect(onPeersChange).toHaveBeenCalledTimes(1)
    const before = hub.peers
    expect(hub.peers).toBe(before)
    const peer = before[0]
    expect(peer?.rev).toBe(0)

    p.send(sub('a'))
    await settled()
    const after = hub.peers
    expect(after).not.toBe(before)
    expect(after[0]).not.toBe(peer)
    expect(after[0]).toMatchObject({ id: 'h#1', rev: 1 })
    expect(after[0]?.channels).toEqual(new Set(['a']))
    expect(peer?.channels.size).toBe(0)
    expect(onPeersChange).toHaveBeenCalledTimes(2)
    expect(onPeersChange).toHaveBeenLastCalledWith(after)

    // A duplicate sub changes nothing.
    p.send(sub('a'))
    await settled()
    expect(hub.peers).toBe(after)
    expect(onPeersChange).toHaveBeenCalledTimes(2)

    p.send({ t: 'unsub', c: 'a' })
    await settled()
    expect(hub.peers[0]?.rev).toBe(2)
    expect(hub.peers[0]?.channels.size).toBe(0)
    expect(onPeersChange).toHaveBeenCalledTimes(3)

    p.link.close()
    expect(hub.peers).toEqual([])
    expect(onPeersChange).toHaveBeenCalledTimes(4)
    expect(onPeersChange).toHaveBeenLastCalledWith([])
  })

  it('Peer.send() reaches that peer and Peer.close() or the accept teardown drops it', async () => {
    const hub = Hub.create<Bus>('h')
    const p = raw(hub)
    const peer = hub.peer('h#1')
    expect(peer?.send('a', 1)).toBe(true)
    await settled()
    expect(p.sent).toEqual([ready('h#1'), data({ c: 'a', d: 1 })])

    peer?.close()
    expect(p.link.up).toBe(false)
    expect(hub.peers).toEqual([])

    const q = raw(hub)
    expect(hub.peers).toHaveLength(1)
    q.off()
    expect(q.link.up).toBe(false)
    expect(hub.peers).toEqual([])
  })

  it('local() sessions become ready and exchange messages through the hub', async () => {
    const hub = Hub.create<Bus>('h')
    const s1 = hub.local()
    const s2 = hub.local()
    await Promise.all([s1.ready(), s2.ready()])
    expect(s1.id).toBe('h#1')
    expect(s2.id).toBe('h#2')

    const on1 = vi.fn()
    const on2 = vi.fn()
    s1.topic('a').listen(on1)
    s2.topic('a').listen(on2)
    await settled()
    expect(hub.peer('h#2')?.channels.has('a')).toBe(true)

    s1.topic('a').send(4)
    await settled()
    expect(on2).toHaveBeenCalledWith(4, { channel: 'a', from: 'h#1', retained: false })
    expect(on1).not.toHaveBeenCalled()

    s1.close()
    expect(hub.peers.map((p) => p.id)).toEqual(['h#2'])
  })

  it("local() takes an explicit validate and keeps the hub's when validate is undefined", async () => {
    const onError = vi.fn()
    const hub = Hub.create<Bus>('h', { validate: { a: (p) => (typeof p === 'number' ? p : undefined) } })
    const inherited = hub.local({ onError })
    const explicitUndefined = hub.local({ onError, validate: undefined })
    const overridden = hub.local({ onError, validate: { a: () => 42 } })
    await Promise.all([inherited.ready(), explicitUndefined.ready(), overridden.ready()])
    const a = vi.fn()
    const b = vi.fn()
    const c = vi.fn()
    inherited.topic('a').listen(a)
    explicitUndefined.topic('a').listen(b)
    overridden.topic('a').listen(c)
    await settled()

    // `publish` bypasses the hub's own check, so the sessions are what narrows here.
    hub.publish('a', 'bad' as unknown as number)
    await settled()

    expect(a).not.toHaveBeenCalled()
    expect(b).not.toHaveBeenCalled()
    expect(c).toHaveBeenCalledWith(42, expect.anything())
    expect(onError).toHaveBeenCalledTimes(2)
    for (const call of onError.mock.calls) expect(call[0]).toBeInstanceOf(Protocol.ProtocolError)
  })

  it('validate rejects payloads before routing and reports a ProtocolError', async () => {
    const onError = vi.fn()
    const hub = Hub.create<Bus>('h', { onError, validate: { a: (p) => (typeof p === 'number' ? p : undefined) } })
    const [p1, p2] = [raw(hub, 'bad-actor'), raw(hub)]
    p2.send(sub('a'))
    await settled()
    p1.send({ t: 'data', c: 'a', d: 'oops' })
    p1.send({ t: 'data', c: 'a', d: 1 })
    await settled()

    expect(p2.sent).toEqual([ready('h#2'), data({ c: 'a', d: 1, from: 'h#1' })])
    expect(onError).toHaveBeenCalledTimes(1)
    const err = onError.mock.calls[0]?.[0] as Protocol.ProtocolError
    expect(err).toBeInstanceOf(Protocol.ProtocolError)
    expect(err.fault).toBe('payload')
    expect(err.detail).toEqual({ hub: 'h', channel: 'a', peer: 'h#1' })
    expect(err.message).toMatch(/bad-actor/)
  })

  it('routes the value the validator returns, not the raw payload', async () => {
    const hub = Hub.create<Bus>('h', { validate: { a: (p) => (typeof p === 'string' ? Number(p) : undefined) } })
    const [p1, p2] = [raw(hub), raw(hub)]
    p2.send(sub('a'))
    await settled()
    p1.send({ t: 'data', c: 'a', d: '5' })
    await settled()
    expect(p2.sent).toEqual([ready('h#2'), data({ c: 'a', d: 5, from: 'h#1' })])
  })

  it('close() closes every peer link, ends local sessions, rejects new accepts and empties peers', async () => {
    const hub = Hub.create<Bus>('h')
    const p = raw(hub)
    const s = hub.local()
    await s.ready()
    const ended = vi.fn()
    s.topic('a').listen(noop, { onClose: ended })

    hub.close()
    hub.close()

    expect(p.link.up).toBe(false)
    expect(s.closed).toBe(true)
    expect(ended).toHaveBeenCalledTimes(1)
    await expect(s.ready()).rejects.toThrow(/is closed/)
    expect(hub.peers).toEqual([])

    const [a, b] = pair<Protocol.Envelope>()
    hub.serve(b)
    expect(a.up).toBe(false)
    expect(hub.peers).toEqual([])
    expect(hub.local().closed).toBe(true)
  })

  it('removes a link that is already dead at accept without greeting or reporting a change', async () => {
    const hub = Hub.create<Bus>('h')
    const onPeersChange = vi.fn()
    hub.onPeersChange(onPeersChange)
    const [a, b] = pair<Protocol.Envelope>()
    a.close()

    expect(() => hub.serve(b)).not.toThrow()
    expect(hub.peers).toEqual([])
    expect(onPeersChange).not.toHaveBeenCalled()

    // A later peer is unaffected.
    const p = raw(hub)
    await settled()
    expect(p.sent).toEqual([ready('h#2')])
    expect(onPeersChange).toHaveBeenCalledTimes(1)
  })

  it('serve() installs a source that its teardown, its signal and close() all stop', async () => {
    const hub = Hub.create<Bus>('h')
    const installed: Array<(link: ILink<Protocol.Envelope>) => void> = []
    const torn = vi.fn()
    const source: ILink.Source<Protocol.Envelope> = (onLink) => {
      installed.push(onLink)
      return torn
    }

    const off = hub.serve(source)
    expect(installed).toHaveLength(1)
    const [mine, theirs] = pair<Protocol.Envelope>('peer', 'h')
    const sent: Protocol.Envelope[] = []
    mine.listen((m) => {
      sent.push(m)
    })
    installed[0]?.(theirs)
    await settled()
    expect(sent).toEqual([ready('h#1')])
    off()
    off()
    expect(torn).toHaveBeenCalledTimes(1)

    const onClose = vi.fn()
    hub.onPeersChange(noop, { onClose })
    hub.serve(source)
    hub.close()
    expect(torn).toHaveBeenCalledTimes(2)
    expect(onClose).toHaveBeenCalledTimes(1)

    // Serving a closed hub is inert, and closes any link handed to it.
    hub.serve(source)
    expect(installed).toHaveLength(2)
  })

  it('closes when its signal aborts, and is born closed when the signal already has', () => {
    const ac = new AbortController()
    const hub = Hub.create<Bus>('h', { signal: ac.signal })
    const peer = raw(hub)
    expect(hub.peers).toHaveLength(1)
    ac.abort()
    expect(hub.peers).toHaveLength(0)
    expect(peer.link.up).toBe(false)

    const dead = new AbortController()
    dead.abort()
    const born = Hub.create<Bus>('h', { signal: dead.signal })
    const [mine, theirs] = pair<Protocol.Envelope>('peer', 'h')
    born.serve(theirs)
    expect(born.peers).toHaveLength(0)
    expect(mine.up).toBe(false)
  })

  it('onPeersChange() fires onClose on hub close, synchronously once closed, and honours a signal', () => {
    const hub = Hub.create<Bus>('h')
    const onClose = vi.fn()
    hub.onPeersChange(noop, { onClose })
    const ac = new AbortController()
    const viaSignal = vi.fn()
    hub.onPeersChange(viaSignal, { signal: ac.signal })
    ac.abort()
    raw(hub)
    expect(viaSignal).not.toHaveBeenCalled()

    hub.close()
    expect(onClose).toHaveBeenCalledTimes(1)

    const late = vi.fn()
    const off = hub.onPeersChange(noop, { onClose: late })
    expect(late).toHaveBeenCalledTimes(1)
    expect(() => off()).not.toThrow()
  })

  it('reports version-mismatch and malformed frames and ignores foreign ones', async () => {
    const onError = vi.fn()
    const hub = Hub.create<Bus>('h', { onError })
    const p = raw(hub, 'stale')
    p.link.send({ t: 'sub', c: 'a', $: 'h', v: 1 } as Protocol.Envelope)
    p.link.send({ t: 'wat', $: 'h', v: Protocol.VERSION } as unknown as Protocol.Envelope)
    p.link.send({ t: 'sub', c: 'a', $: 'other', v: Protocol.VERSION })
    await settled()

    expect(onError).toHaveBeenCalledTimes(2)
    const [version, malformed] = onError.mock.calls.map((c) => c[0] as Protocol.ProtocolError)
    expect(version).toBeInstanceOf(Protocol.ProtocolError)
    expect(version?.fault).toBe('version')
    expect(version?.detail).toEqual({ hub: 'h', peer: 'h#1', version: 1 })
    expect(version?.message).toMatch(/stale/)
    expect(malformed?.fault).toBe('malformed')
    expect(hub.peer('h#1')?.channels.size).toBe(0)
  })
})
