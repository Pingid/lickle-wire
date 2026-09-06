import { describe, expect, it, vi } from 'vitest'
import { define } from './define.js'
import * as Protocol from './protocol.ts'
import type { Hub } from './hub.js'
import { noop } from '../core/internal.ts'
import { pair, type ILink } from './link/index.ts'
import type { Session } from './session.js'
import { fakeClock } from './testing.js'

type Bus = { a: number; b: string }

/** Drains the microtask chains `Link.pair` delivers on. */
const settled = () => new Promise<void>((r) => setTimeout(r, 0))

const numberOnly = (p: unknown) => (typeof p === 'number' ? p : undefined)
const app = define<Bus>({ name: 'app', validate: { a: numberOnly } })

/** A raw peer of an `app` hub. */
const raw = (hub: Hub<Bus>) => {
  const [mine, theirs] = pair<Protocol.Envelope>('peer', hub.name)
  const sent: Protocol.Envelope[] = []
  mine.listen((m) => {
    sent.push(m)
  })
  hub.serve(theirs)
  return { sent, send: (f: Protocol.Frame) => mine.send(Protocol.seal(hub.name, f)) }
}

/** A session on one end of a pair, greeted by hand. */
const greeted = <S extends Session<Bus, ILink<Protocol.Envelope>>>(make: (link: ILink<Protocol.Envelope>) => S) => {
  const [mine, theirs] = pair<Protocol.Envelope>('client', 'app')
  const session = make(mine)
  const fn = vi.fn()
  session.topic('a').listen(fn)
  theirs.send(Protocol.seal('app', { t: 'ready', id: 'p1' }))
  return { session, fn, push: (f: Protocol.Frame) => theirs.send(Protocol.seal('app', f)) }
}

const ready = (id: string) => Protocol.seal('app', { t: 'ready', id })

const faults = (onError: ReturnType<typeof vi.fn>) =>
  onError.mock.calls.map((c) => {
    const err = c[0] as Protocol.ProtocolError
    expect(err).toBeInstanceOf(Protocol.ProtocolError)
    return err.fault
  })

describe('define()', () => {
  it('exposes the name', () => {
    const name: string = app.name
    expect(name).toBe('app')
  })

  it('hub() inherits the spec validators', async () => {
    const onError = vi.fn()
    const hub = app.hub({ onError })
    const [p1, p2] = [raw(hub), raw(hub)]
    p2.send({ t: 'sub', c: 'a' })
    await settled()
    p1.send({ t: 'data', c: 'a', d: 'bad' })
    p1.send({ t: 'data', c: 'a', d: 1 })
    await settled()

    expect(p2.sent).toEqual([ready('app#2'), Protocol.seal('app', { t: 'data', c: 'a', d: 1, from: 'app#1' })])
    expect(faults(onError)).toEqual(['payload'])
  })

  it('session() inherits the spec validators', async () => {
    const onError = vi.fn()
    const { fn, push } = greeted((link) => app.session(link, { onError }))
    push({ t: 'data', c: 'a', d: 'bad' })
    push({ t: 'data', c: 'a', d: 1 })
    await settled()

    expect(fn).toHaveBeenCalledTimes(1)
    expect(fn).toHaveBeenCalledWith(1, expect.anything())
    expect(faults(onError)).toEqual(['payload'])
  })

  it('keeps the spec validators when validate is explicitly undefined (hub, session, connect)', async () => {
    const onError = vi.fn()

    const hub = app.hub({ onError, validate: undefined })
    const [p1, p2] = [raw(hub), raw(hub)]
    p2.send({ t: 'sub', c: 'a' })
    await settled()
    p1.send({ t: 'data', c: 'a', d: 'bad' })
    await settled()
    expect(p2.sent).toEqual([ready('app#2')])

    const viaSession = greeted((link) => app.session(link, { onError, validate: undefined }))
    viaSession.push({ t: 'data', c: 'a', d: 'bad' })
    await settled()
    expect(viaSession.fn).not.toHaveBeenCalled()

    const viaConnect = greeted((link) => app.connect(() => link, { onError, validate: undefined, clock: fakeClock() }))
    viaConnect.push({ t: 'data', c: 'a', d: 'bad' })
    await settled()
    expect(viaConnect.session.connected).toBe(true)
    expect(viaConnect.fn).not.toHaveBeenCalled()

    expect(faults(onError)).toEqual(['payload', 'payload', 'payload'])
  })

  it('lets an explicit validate override the spec', async () => {
    const hub = app.hub({ validate: { a: () => 42 } })
    const [p1, p2] = [raw(hub), raw(hub)]
    p2.send({ t: 'sub', c: 'a' })
    await settled()
    p1.send({ t: 'data', c: 'a', d: 'anything' })
    await settled()
    expect(p2.sent).toEqual([ready('app#2'), Protocol.seal('app', { t: 'data', c: 'a', d: 42, from: 'app#1' })])

    const { fn, push } = greeted((link) => app.session(link, { validate: { a: () => 7 } }))
    push({ t: 'data', c: 'a', d: 'anything' })
    await settled()
    expect(fn).toHaveBeenCalledWith(7, expect.anything())
  })

  it('connect() returns a session over a persistent link that a hub can accept', async () => {
    const hub = app.hub()
    const session = app.connect(
      () => {
        const [mine, theirs] = pair<Protocol.Envelope>('client', 'app')
        hub.serve(theirs)
        return mine
      },
      { clock: fakeClock() },
    )
    expect(session.link.state).toBe('connecting')
    expect(typeof session.link.retryNow).toBe('function')
    expect(session.connected).toBe(false)

    await session.ready()
    expect(session.link.state).toBe('open')
    expect(session.link.up).toBe(true)
    expect(session.id).toBe('app#1')

    const other = hub.local()
    const fn = vi.fn()
    other.topic('a').listen(fn)
    await settled()
    session.topic('a').send(1)
    await settled()
    expect(fn).toHaveBeenCalledWith(1, { channel: 'a', from: 'app#1', retained: false })

    session.close()
    expect(session.link.state).toBe('closed')
    expect(hub.peers.map((p) => p.id)).toEqual(['app#2'])
    hub.close()
  })

  it('connect() ends the session when the persistent link gives up', async () => {
    const clock = fakeClock()
    const onError = vi.fn()
    const onClose = vi.fn()
    // Nobody ever greets this connector.
    const session = app.connect(() => pair<Protocol.Envelope>('client', 'app')[0], {
      clock,
      timeout: 10,
      maxAttempts: 1,
      onError,
      onClose,
    })
    const ended = vi.fn()
    session.topic('a').listen(noop, { onClose: ended })
    const pending = session.ready()

    clock.advance(10)

    expect(onError.mock.calls.map((c) => (c[0] as Error).message)).toEqual([
      expect.stringMatching(/no greeting/),
      expect.stringMatching(/giving up/),
    ])
    expect(session.link.state).toBe('closed')
    expect(session.link.attempts).toBe(1)
    expect(session.closed).toBe(true)
    // It never connected, so there is no close event — only the terminal onClose.
    expect(onClose).not.toHaveBeenCalled()
    expect(ended).toHaveBeenCalledTimes(1)
    await expect(pending).rejects.toThrow(/is closed/)
    expect(clock.pending).toBe(0)
  })
})
