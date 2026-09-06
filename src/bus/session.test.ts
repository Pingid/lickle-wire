import { describe, expect, it, vi } from 'vitest'
import * as Protocol from './protocol.ts'
import { noop } from '../core/internal.ts'
import { baseLink } from './link/base.ts'
import { pair } from './link/index.ts'
import { Session } from './session.ts'

const { seal, VERSION, ProtocolError } = Protocol
type ProtocolError = InstanceType<typeof ProtocolError>

type Bus = { a: number; b: string; x: unknown }

/** Drains the microtask chains `Link.pair` delivers on. */
const settled = () => new Promise<void>((r) => setTimeout(r, 0))

/** A session on one end of a pair; the hub's end is driven by hand. */
const harness = (opts: Session.Options<Bus> = {}) => {
  const [mine, theirs] = pair<Protocol.Envelope>('client', 'hub')
  const sent: Protocol.Envelope[] = []
  theirs.listen((m) => {
    sent.push(m)
  })
  const session = Session.over<Bus>('h', mine, opts)
  return {
    session,
    link: mine,
    theirs,
    /** Every envelope the session has put on the wire. */
    sent,
    /** The hub's greeting. */
    greet: (id = 'p1') => theirs.send(seal('h', { t: 'ready', id })),
    /** Any frame, as the hub would send it. */
    push: (f: Protocol.Frame) => theirs.send(seal('h', f)),
  }
}

/** A link whose `up` can be toggled without closing it — what `Link.persistent` does while retrying. */
const stub = () => {
  const sent: Protocol.Envelope[] = []
  const core = baseLink<Protocol.Envelope>({ describe: () => ({ remote: 'hub', meta: {} }) })
  const link = core.expose(
    (m) => {
      sent.push(m)
      return true
    },
    () => core.shut(),
  )
  return { core, link, sent }
}

const sub = (c: string) => seal('h', { t: 'sub', c })
const unsub = (c: string) => seal('h', { t: 'unsub', c })

describe('Session', () => {
  it('is not connected until the hub greets it', async () => {
    const onOpen = vi.fn()
    const { session, greet } = harness({ onOpen })
    expect(session.connected).toBe(false)
    expect(session.id).toBeNull()
    expect(onOpen).not.toHaveBeenCalled()

    greet('p1')
    await settled()

    expect(session.connected).toBe(true)
    expect(session.id).toBe('p1')
    expect(onOpen).toHaveBeenCalledTimes(1)
  })

  it('ready() resolves on the greeting, and immediately when already connected', async () => {
    const { session, greet } = harness()
    const pending = session.ready()
    greet()
    await expect(pending).resolves.toBeUndefined()
    await expect(session.ready()).resolves.toBeUndefined()
  })

  it('ready() rejects with the reason when its signal aborts', async () => {
    const { session } = harness()
    const ac = new AbortController()
    const pending = session.ready({ signal: ac.signal })
    ac.abort(new Error('gave up waiting'))
    await expect(pending).rejects.toThrow('gave up waiting')

    const early = new AbortController()
    early.abort(new Error('already gone'))
    await expect(session.ready({ signal: early.signal })).rejects.toThrow('already gone')
  })

  it('on() honours a signal, returns a teardown, and reports faults on "error"', async () => {
    const { session, greet, theirs } = harness()
    const ac = new AbortController()
    const viaSignal = vi.fn()
    const viaTeardown = vi.fn()
    const kept = vi.fn()
    const fault = vi.fn()
    session.on('open', viaSignal, { signal: ac.signal })
    const off = session.on('open', viaTeardown)
    session.on('open', kept)
    session.on('error', fault)
    ac.abort()
    off()

    greet()
    theirs.send({ t: 'ready', id: 'x', $: 'h', v: 1 } as Protocol.Envelope)
    await settled()

    expect(viaSignal).not.toHaveBeenCalled()
    expect(viaTeardown).not.toHaveBeenCalled()
    expect(kept).toHaveBeenCalledTimes(1)
    expect(fault).toHaveBeenCalledTimes(1)
    expect(fault.mock.calls[0]?.[0]).toBeInstanceOf(ProtocolError)
  })

  it('on() fires onClose when the session ends, synchronously if it already has', () => {
    const { session } = harness()
    const onClose = vi.fn()
    session.on('open', noop, { onClose })
    session.close()
    expect(onClose).toHaveBeenCalledTimes(1)

    const late = vi.fn()
    const off = session.on('open', noop, { onClose: late })
    expect(late).toHaveBeenCalledTimes(1)
    expect(() => off()).not.toThrow()
  })

  it('reports version and malformed frames and ignores foreign ones', async () => {
    const onError = vi.fn()
    const { theirs } = harness({ onError })
    theirs.send({ t: 'ready', id: 'x', $: 'h', v: 1 } as Protocol.Envelope)
    theirs.send({ t: 'nope', $: 'h', v: VERSION } as unknown as Protocol.Envelope)
    theirs.send({ t: 'ready', id: 'x', $: 'other', v: VERSION })
    theirs.send('not even an object' as unknown as Protocol.Envelope)
    await settled()

    expect(onError).toHaveBeenCalledTimes(2)
    const [version, malformed] = onError.mock.calls.map((c) => c[0] as ProtocolError)
    expect(version).toBeInstanceOf(ProtocolError)
    expect(version?.fault).toBe('version')
    expect(version?.detail).toEqual({ hub: 'h', version: 1 })
    expect(version?.message).toMatch(/version frame from hub/)
    expect(malformed?.fault).toBe('malformed')
  })

  it('ends when its link closes: close event, listener onClose, ready() rejects', async () => {
    const onClose = vi.fn()
    const { session, theirs, link, greet } = harness({ onClose })
    greet()
    await settled()
    const closeEvent = vi.fn()
    const ended = vi.fn()
    session.on('close', closeEvent)
    session.topic('a').listen(noop, { onClose: ended })

    theirs.close()

    expect(session.closed).toBe(true)
    expect(session.connected).toBe(false)
    expect(session.id).toBeNull()
    expect(link.up).toBe(false)
    expect(onClose).toHaveBeenCalledTimes(1)
    expect(closeEvent).toHaveBeenCalledTimes(1)
    expect(ended).toHaveBeenCalledTimes(1)
    await expect(session.ready()).rejects.toThrow(/is closed/)
  })

  it('still ends, without a close event, when the link dies before any greeting', async () => {
    const onClose = vi.fn()
    const { session, theirs } = harness({ onClose })
    const closeEvent = vi.fn()
    const ended = vi.fn()
    session.on('close', closeEvent)
    session.topic('a').listen(noop, { onClose: ended })
    const pending = session.ready()

    theirs.close()

    expect(session.closed).toBe(true)
    expect(onClose).not.toHaveBeenCalled()
    expect(closeEvent).not.toHaveBeenCalled()
    expect(ended).toHaveBeenCalledTimes(1)
    await expect(pending).rejects.toThrow(/is closed/)
  })

  it('is born closed when handed a link that is already closed', async () => {
    const [mine] = pair<Protocol.Envelope>()
    mine.close()
    const onClose = vi.fn()
    const onOpen = vi.fn()
    const session = Session.over<Bus>('h', mine, { onClose, onOpen })

    expect(session.closed).toBe(true)
    expect(session.connected).toBe(false)
    await expect(session.ready()).rejects.toThrow(/is closed/)
    const ended = vi.fn()
    session.topic('a').listen(noop, { onClose: ended })
    expect(ended).toHaveBeenCalledTimes(1)
    expect(session.topic('a').send(1)).toBe(false)
    expect(onClose).not.toHaveBeenCalled()
    expect(onOpen).not.toHaveBeenCalled()
  })

  it('close() is idempotent and closes the link it owns', async () => {
    const onClose = vi.fn()
    const { session, link, greet } = harness({ onClose })
    greet()
    await settled()
    const ended = vi.fn()
    session.topic('a').listen(noop, { onClose: ended })

    session.close()
    session.close()

    expect(link.up).toBe(false)
    expect(session.closed).toBe(true)
    expect(onClose).toHaveBeenCalledTimes(1)
    expect(ended).toHaveBeenCalledTimes(1)
  })

  it('goes down without ending when the link reports up: false, and reconnects on the next ready', async () => {
    const { core, link, sent } = stub()
    const onClose = vi.fn()
    const onOpen = vi.fn()
    const session = Session.over<Bus>('h', link, { onClose, onOpen })
    const ended = vi.fn()
    session.topic('a').listen(noop, { onClose: ended })
    core.deliver(seal('h', { t: 'ready', id: 'p1' }))
    expect(session.connected).toBe(true)
    expect(sent).toEqual([sub('a')])

    core.signal(false)

    expect(onClose).toHaveBeenCalledTimes(1)
    expect(session.closed).toBe(false)
    expect(session.connected).toBe(false)
    expect(session.id).toBeNull()
    expect(ended).not.toHaveBeenCalled()

    // Intent gathered while down is kept, not sent.
    session.topic('b').listen(noop)
    expect(sent).toEqual([sub('a')])

    let outcome = 'pending'
    const pending = session.ready().then(
      () => (outcome = 'resolved'),
      () => (outcome = 'rejected'),
    )
    await settled()
    expect(outcome).toBe('pending')

    core.deliver(seal('h', { t: 'ready', id: 'p2' }))
    await pending
    expect(outcome).toBe('resolved')
    expect(session.connected).toBe(true)
    expect(session.id).toBe('p2')
    expect(onOpen).toHaveBeenCalledTimes(2)
    expect(sent).toEqual([sub('a'), sub('a'), sub('b')])
  })
})

describe('Session.topic()', () => {
  it('sends sub on the first listener and unsub when the last leaves', async () => {
    const { session, sent, greet } = harness()
    greet()
    await settled()
    const t = session.topic('a')
    const off1 = t.listen(noop)
    const off2 = t.listen(noop)
    await settled()
    expect(sent).toEqual([sub('a')])

    off1()
    off1()
    await settled()
    expect(sent).toEqual([sub('a')])

    off2()
    await settled()
    expect(sent).toEqual([sub('a'), unsub('a')])
  })

  it('sends nothing while not ready and replays intent on ready', async () => {
    const { session, sent, greet } = harness()
    const offB = session.topic('b').listen(noop)
    session.topic('a').listen(noop)
    await settled()
    expect(sent).toEqual([])

    // Intent withdrawn before any greeting: no unsub either.
    offB()
    await settled()
    expect(sent).toEqual([])

    greet()
    await settled()
    expect(sent).toEqual([sub('a')])
  })

  it('refcounts per receive channel across topics that read it', async () => {
    const { session, sent, greet, push } = harness()
    greet()
    await settled()
    const onA = vi.fn()
    const onBA = vi.fn()
    const offA = session.topic('a').listen(onA)
    const offBA = session.topic('b', 'a').listen(onBA)
    await settled()
    expect(sent).toEqual([sub('a')])

    push({ t: 'data', c: 'a', d: 1 })
    await settled()
    expect(onA).toHaveBeenCalledWith(1, expect.anything())
    expect(onBA).toHaveBeenCalledWith(1, expect.anything())

    offA()
    await settled()
    expect(sent).toEqual([sub('a')])
    offBA()
    await settled()
    expect(sent).toEqual([sub('a'), unsub('a')])
  })

  it('delivers payloads with channel, from and retained meta', async () => {
    const { session, greet, push } = harness()
    greet()
    const fn = vi.fn()
    session.topic('a').listen(fn)
    push({ t: 'data', c: 'a', d: 1, from: 'p2' })
    push({ t: 'data', c: 'a', d: 2, r: true })
    push({ t: 'data', c: 'b', d: 'unheard' })
    await settled()

    expect(fn).toHaveBeenCalledTimes(2)
    expect(fn).toHaveBeenNthCalledWith(1, 1, { channel: 'a', from: 'p2', retained: false })
    expect(fn).toHaveBeenNthCalledWith(2, 2, { channel: 'a', from: null, retained: true })
  })

  it('send() frames the publish channel and an optional recipient', async () => {
    const { session, sent } = harness()
    const t = session.topic('b', 'a')
    expect(t.to).toBe('b')
    expect(t.from).toBe('a')
    expect(t.send('hi')).toBe(true)
    expect(t.send('you', { to: 'p2' })).toBe(true)
    await settled()
    expect(sent).toEqual([
      seal('h', { t: 'data', c: 'b', d: 'hi' }),
      seal('h', { t: 'data', c: 'b', d: 'you', to: 'p2' }),
    ])
  })

  it('send() returns false once the session is closed', () => {
    const { session } = harness()
    const t = session.topic('a')
    session.close()
    expect(t.send(1)).toBe(false)
    expect(session.topic('a').send(1)).toBe(false)
  })

  it('validates inbound payloads and reports rejections as payload faults', async () => {
    const onError = vi.fn()
    const { session, greet, push } = harness({
      onError,
      validate: { a: (p) => (typeof p === 'number' ? p : undefined) },
    })
    greet()
    const fn = vi.fn()
    session.topic('a').listen(fn)
    push({ t: 'data', c: 'a', d: 'nope', from: 'p2' })
    push({ t: 'data', c: 'a', d: 5, from: 'p2' })
    await settled()

    expect(fn).toHaveBeenCalledTimes(1)
    expect(fn).toHaveBeenCalledWith(5, expect.anything())
    expect(onError).toHaveBeenCalledTimes(1)
    const err = onError.mock.calls[0]?.[0] as ProtocolError
    expect(err).toBeInstanceOf(ProtocolError)
    expect(err.fault).toBe('payload')
    expect(err.detail).toEqual({ hub: 'h', channel: 'a', peer: 'p2' })
  })

  it('reports a throwing listener without starving the rest', async () => {
    const onError = vi.fn()
    const { session, greet, push } = harness({ onError })
    greet()
    const t = session.topic('a')
    t.listen(() => {
      throw new Error('boom')
    })
    const second = vi.fn()
    t.listen(second)
    push({ t: 'data', c: 'a', d: 1 })
    await settled()

    expect(second).toHaveBeenCalledWith(1, expect.anything())
    expect(onError).toHaveBeenCalledTimes(1)
    expect(onError.mock.calls[0]?.[0]).toEqual(new Error('boom'))
  })

  it('listen() honours a signal and takes the subscription with it', async () => {
    const { session, sent, greet } = harness()
    greet()
    await settled()
    const ac = new AbortController()
    session.topic('a').listen(noop, { signal: ac.signal })
    await settled()
    expect(sent).toEqual([sub('a')])
    ac.abort()
    await settled()
    expect(sent).toEqual([sub('a'), unsub('a')])

    const gone = new AbortController()
    gone.abort()
    session.topic('b').listen(noop, { signal: gone.signal })
    await settled()
    expect(sent).toEqual([sub('a'), unsub('a')])
  })

  it('once() detaches after the first payload', async () => {
    const { session, sent, greet, push } = harness()
    greet()
    await settled()
    const fn = vi.fn()
    session.topic('a').once(fn)
    push({ t: 'data', c: 'a', d: 1 })
    push({ t: 'data', c: 'a', d: 2 })
    await settled()

    expect(fn).toHaveBeenCalledTimes(1)
    expect(fn).toHaveBeenCalledWith(1, expect.anything())
    expect(sent).toEqual([sub('a'), unsub('a')])
  })

  it('listen() fires onClose once when the session ends and not after unsubscribing', () => {
    const { session } = harness()
    const t = session.topic('a')
    const kept = vi.fn()
    const left = vi.fn()
    t.listen(noop, { onClose: kept })
    t.listen(noop, { onClose: left })()
    session.close()
    session.close()
    expect(kept).toHaveBeenCalledTimes(1)
    expect(left).not.toHaveBeenCalled()
  })

  it('listen() on a closed session fires onClose synchronously and returns a noop', () => {
    const { session } = harness()
    const t = session.topic('a')
    session.close()
    const onClose = vi.fn()
    const off = t.listen(noop, { onClose })
    expect(onClose).toHaveBeenCalledTimes(1)
    expect(() => off()).not.toThrow()
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  describe('stream()', () => {
    it('yields payloads in order, buffering while the consumer is slow', async () => {
      const { session, greet, push } = harness()
      greet()
      const stream = session.topic('a').stream()
      push({ t: 'data', c: 'a', d: 1 })
      push({ t: 'data', c: 'a', d: 2 })
      push({ t: 'data', c: 'a', d: 3 })
      await settled()
      expect(await stream.next()).toEqual({ value: 1, done: false })
      expect(await stream.next()).toEqual({ value: 2, done: false })
      expect(await stream.next()).toEqual({ value: 3, done: false })
    })

    it('drops the oldest payloads past buffer', async () => {
      const { session, greet, push } = harness()
      greet()
      const stream = session.topic('a').stream({ buffer: 2 })
      push({ t: 'data', c: 'a', d: 1 })
      push({ t: 'data', c: 'a', d: 2 })
      push({ t: 'data', c: 'a', d: 3 })
      await settled()
      expect(await stream.next()).toEqual({ value: 2, done: false })
      expect(await stream.next()).toEqual({ value: 3, done: false })
    })

    it('ends when the session closes and forwards onClose', async () => {
      const { session } = harness()
      const onClose = vi.fn()
      const stream = session.topic('a').stream({ onClose })
      const pending = stream.next()
      session.close()
      await expect(pending).resolves.toEqual({ value: undefined, done: true })
      await expect(stream.next()).resolves.toEqual({ value: undefined, done: true })
      expect(onClose).toHaveBeenCalledTimes(1)
    })

    it('releases its subscription when the consumer returns early or aborts', async () => {
      const { session, sent, greet } = harness()
      greet()
      await settled()
      const t = session.topic('a')
      const first = t.stream()
      await settled()
      expect(sent).toEqual([sub('a')])
      await first.return?.()
      await settled()
      expect(sent).toEqual([sub('a'), unsub('a')])

      const ac = new AbortController()
      const second = t.stream({ signal: ac.signal })
      await settled()
      expect(sent).toEqual([sub('a'), unsub('a'), sub('a')])
      ac.abort()
      await settled()
      expect(sent).toEqual([sub('a'), unsub('a'), sub('a'), unsub('a')])
      await expect(second.next()).resolves.toEqual({ value: undefined, done: true })
    })
  })

  describe('peer()', () => {
    it('addresses sends to the peer and delivers only its frames', async () => {
      const { session, sent, greet, push } = harness()
      greet()
      await settled()
      const port = session.topic('a').peer('p2')
      port.send(7)
      const fn = vi.fn()
      port.listen(fn)
      await settled()
      expect(sent).toEqual([seal('h', { t: 'data', c: 'a', d: 7, to: 'p2' }), sub('a')])

      push({ t: 'data', c: 'a', d: 1, from: 'p2' })
      push({ t: 'data', c: 'a', d: 2 })
      push({ t: 'data', c: 'a', d: 3, from: 'p3' })
      push({ t: 'data', c: 'a', d: 4, r: true })
      await settled()

      expect(fn).toHaveBeenCalledTimes(1)
      expect(fn).toHaveBeenCalledWith(1)
    })

    it('shares the channel refcount and ends with the session', async () => {
      const { session, sent, greet } = harness()
      greet()
      await settled()
      const t = session.topic('a')
      const offPeer = t.peer('p2').listen(noop)
      const offPlain = t.listen(noop)
      await settled()
      expect(sent).toEqual([sub('a')])

      offPeer()
      await settled()
      expect(sent).toEqual([sub('a')])
      offPlain()
      await settled()
      expect(sent).toEqual([sub('a'), unsub('a')])

      const onClose = vi.fn()
      t.peer('p2').listen(noop, { onClose })
      session.close()
      expect(onClose).toHaveBeenCalledTimes(1)
    })
  })
})
