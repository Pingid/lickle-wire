import { describe, expect, it, vi } from 'vitest'
import { fakeClock } from '../testing.ts'
import { Hub } from '../hub.ts'
import type { Envelope } from '../protocol.ts'
import { Session } from '../session.ts'
import { defineLink } from './base.ts'
import { pair } from './pair.ts'
import { slot } from './slot.ts'
import type { Link } from './index.ts'

/** `pair` delivers on a microtask; this outlasts a round trip through a hub. */
const settled = () => new Promise<void>((r) => setTimeout(r, 0))

/** A link whose transport side is driven by hand. */
const wire = (remote = 'a', up = true) => {
  const sent: string[] = []
  let host!: Link.Host<string>
  let released = false
  const link = defineLink<string>(
    (h) => {
      host = h
      return {
        send: (m) => {
          sent.push(m)
          return true
        },
        release: () => (released = true),
      }
    },
    { remote, meta: { origin: remote }, up },
  )
  return {
    link,
    sent,
    host: host!,
    get released() {
      return released
    },
  }
}

describe('slot', () => {
  it('keeps every subscription across use, teardown and re-use', () => {
    const s = slot<string>()
    const got: string[] = []
    const ups: boolean[] = []
    const ended = vi.fn()

    // Subscribed once, before anything is bound.
    s.listen((m) => got.push(m))
    s.changed((up) => ups.push(up))
    s.closed(ended)

    const a = wire('a')
    const offA = s.use(a.link)
    a.host.deliver('from-a')

    offA()
    const b = wire('b')
    s.use(b.link)
    b.host.deliver('from-b')

    // One `listen` call, messages from two different transports.
    expect(got).toEqual(['from-a', 'from-b'])
    expect(ups).toEqual([true, false, true])
    expect(ended).not.toHaveBeenCalled()
  })

  it('reports the inner while bound and the fallback while not', () => {
    const s = slot<string>({ remote: 'idle', meta: { origin: 'none' } })
    expect(s.remote).toBe('idle')
    expect(s.meta.origin).toBe('none')
    expect(s.inner).toBeNull()
    expect(s.up).toBe(false)

    const a = wire('a')
    const off = s.use(a.link)
    expect(s.remote).toBe('a')
    expect(s.meta.origin).toBe('a')
    expect(s.inner).toBe(a.link)
    expect(s.up).toBe(true)

    off()
    expect(s.remote).toBe('idle')
    expect(s.inner).toBeNull()
    expect(s.up).toBe(false)
  })

  it('tracks the inner going up and down without ending', () => {
    const s = slot<string>()
    const ups: boolean[] = []
    s.changed((up) => ups.push(up))

    const a = wire('a', false)
    s.use(a.link)
    expect(s.up).toBe(false)

    a.host.signal(true)
    expect(s.up).toBe(true)
    a.host.signal(false)
    expect(s.up).toBe(false)
    expect(ups).toEqual([true, false])
  })

  it('unbinds itself when the inner closes, and stays alive', () => {
    const onInner = vi.fn()
    const s = slot<string>({ onInner })
    const ended = vi.fn()
    s.closed(ended)

    const a = wire('a')
    s.use(a.link)
    a.link.close()

    expect(s.inner).toBeNull()
    expect(s.up).toBe(false)
    expect(ended).not.toHaveBeenCalled()
    expect(onInner.mock.calls.map((c) => c[0])).toEqual([a.link, null])

    // ...and it takes a new one.
    const b = wire('b')
    s.use(b.link)
    expect(s.up).toBe(true)
    expect(s.remote).toBe('b')
  })

  it('stops listening to a link it let go', () => {
    const s = slot<string>()
    const got: string[] = []
    s.listen((m) => got.push(m))

    const a = wire('a')
    s.use(a.link)()

    // The old transport is still live, but no longer ours.
    a.host.deliver('stale')
    a.host.signal(false)
    expect(got).toEqual([])
    expect(s.up).toBe(false)
  })

  it('using another detaches the previous one', () => {
    const s = slot<string>()
    const got: string[] = []
    s.listen((m) => got.push(m))

    const a = wire('a')
    const b = wire('b')
    s.use(a.link)
    s.use(b.link)

    a.host.deliver('stale')
    b.host.deliver('live')
    expect(got).toEqual(['live'])
    expect(s.inner).toBe(b.link)
  })

  it('using what is already in use does not rebuild the binding', () => {
    const onInner = vi.fn()
    const s = slot<string>({ onInner })
    const a = wire('a')
    const first = s.use(a.link)
    const second = s.use(a.link)
    expect(onInner).toHaveBeenCalledTimes(1)
    expect(a.released).toBe(false)

    // Either teardown detaches the one binding they both name.
    second()
    expect(s.inner).toBeNull()
    first()
    expect(s.inner).toBeNull()
  })

  it('hands back a teardown that is inert once something else is in use', () => {
    const s = slot<string>()
    const a = wire('a')
    const b = wire('b')
    const offA = s.use(a.link)
    s.use(b.link)

    // `a` is long gone; detaching it late must not cut `b`'s wire.
    offA()
    expect(s.inner).toBe(b.link)
    expect(s.up).toBe(true)
  })

  it('holds sends while unbound and flushes them on bind', () => {
    const s = slot<string>()
    expect(s.send('one')).toBe(true)
    expect(s.send('two')).toBe(true)

    const a = wire('a')
    s.use(a.link)
    expect(a.sent).toEqual(['one', 'two'])

    // Bound and up: straight through.
    s.send('three')
    expect(a.sent).toEqual(['one', 'two', 'three'])
  })

  it('holds sends while the inner is down, and flushes when it comes back up', () => {
    const s = slot<string>()
    const a = wire('a', false)
    s.use(a.link)

    s.send('held')
    expect(a.sent).toEqual([])
    a.host.signal(true)
    expect(a.sent).toEqual(['held'])
  })

  it('refuses a full buffer at the call site, and expires by ttl', () => {
    const onDrop = vi.fn()
    const clock = fakeClock()
    const s = slot<string>({ buffer: 2, onDrop, clock })

    expect(s.send('a')).toBe(true)
    expect(s.send('b')).toBe(true)
    expect(s.send('c')).toBe(false)
    expect(onDrop).toHaveBeenCalledWith('c')

    const oldest = slot<string>({ buffer: 2, overflow: 'oldest', onDrop, clock })
    oldest.send('a')
    oldest.send('b')
    expect(oldest.send('c')).toBe(true)
    const w = wire('w')
    oldest.use(w.link)
    expect(w.sent).toEqual(['b', 'c'])

    const stale = slot<string>({ ttl: 100, clock, onDrop })
    stale.send('old')
    clock.advance(101)
    stale.send('new')
    const x = wire('x')
    stale.use(x.link)
    expect(x.sent).toEqual(['new'])
  })

  it('never buffers with buffer: 0', () => {
    const onDrop = vi.fn()
    const s = slot<string>({ buffer: 0, onDrop })
    expect(s.send('x')).toBe(false)
    expect(onDrop).toHaveBeenCalledWith('x')

    const a = wire('a')
    s.use(a.link)
    expect(a.sent).toEqual([])
  })

  it('buffers inbound that arrives before the first listener', () => {
    const s = slot<string>()
    const a = wire('a')
    s.use(a.link)
    a.host.deliver('early')

    const got = vi.fn()
    s.listen(got)
    expect(got).toHaveBeenCalledWith('early')
  })

  it('leaves the inner alone by default, and closes it under own', () => {
    const loose = slot<string>()
    const a = wire('a')
    loose.use(a.link)()
    expect(a.link.up).toBe(true)
    loose.close()
    expect(a.link.up).toBe(true)

    const owned = slot<string>({ own: true })
    const b = wire('b')
    owned.use(b.link)()
    expect(b.link.up).toBe(false)

    const c = wire('c')
    const d = wire('d')
    const owner = slot<string>({ own: true })
    owner.use(c.link)
    owner.use(d.link) // swapping closes the one it replaced
    expect(c.link.up).toBe(false)
    owner.close()
    expect(d.link.up).toBe(false)
  })

  it('ends only when it is closed, not when a transport does', () => {
    const s = slot<string>()
    const ended = vi.fn()
    s.closed(ended)

    const a = wire('a')
    s.use(a.link)
    a.link.close()
    expect(ended).not.toHaveBeenCalled()

    s.close()
    expect(ended).toHaveBeenCalledTimes(1)
    expect(s.up).toBe(false)
    expect(s.send('x')).toBe(false)

    s.close()
    expect(ended).toHaveBeenCalledTimes(1)
  })

  it('takes nothing once closed, and does not strand what it is handed', () => {
    const loose = slot<string>()
    loose.close()
    const a = wire('a')
    expect(() => loose.use(a.link)()).not.toThrow()
    expect(loose.inner).toBeNull()
    expect(a.link.up).toBe(true) // not ours to close

    const owned = slot<string>({ own: true })
    owned.close()
    const b = wire('b')
    owned.use(b.link)
    expect(owned.inner).toBeNull()
    expect(b.link.up).toBe(false)
  })

  it('drops what is still held when it closes', () => {
    const onDrop = vi.fn()
    const s = slot<string>({ onDrop })
    s.send('never')
    s.close()
    expect(onDrop).toHaveBeenCalledWith('never')
  })

  it('survives being handed a link that is already dead', () => {
    const onInner = vi.fn()
    const s = slot<string>({ onInner })
    const ended = vi.fn()
    s.closed(ended)

    const a = wire('a')
    a.link.close()
    s.use(a.link)

    // `closed` fires synchronously from inside `bind`; the slot unbinds itself
    // rather than ending, and is ready for the next one.
    expect(s.inner).toBeNull()
    expect(s.up).toBe(false)
    expect(ended).not.toHaveBeenCalled()
    expect(onInner.mock.calls.map((c) => c[0])).toEqual([a.link, null])

    const b = wire('b')
    s.use(b.link)
    expect(s.up).toBe(true)
  })

  it('is a plain Link to everything above it', () => {
    const s = slot<string>()
    const session: Link<string> = s
    const a = wire('a')
    s.use(a.link)

    const got = vi.fn()
    const off = session.listen(got)
    a.host.deliver('x')
    off()
    a.host.deliver('y')
    expect(got).toHaveBeenCalledTimes(1)
  })

  it('honours an abort signal on subscriptions across binds', () => {
    const s = slot<string>()
    const ac = new AbortController()
    const got = vi.fn()
    s.listen(got, { signal: ac.signal })

    const a = wire('a')
    s.use(a.link)
    a.host.deliver('one')

    ac.abort()
    s.use(wire('b').link)
    expect(got).toHaveBeenCalledTimes(1)
  })

  it('reports a throwing onInner without losing the bind', () => {
    const onError = vi.fn()
    const s = slot<string>({
      onError,
      onInner: () => {
        throw new Error('boom')
      },
    })
    const a = wire('a')
    s.use(a.link)
    expect(onError).toHaveBeenCalledTimes(1)
    expect(s.inner).toBe(a.link)
    expect(s.up).toBe(true)
  })

  it('carries a live session from one hub to another', async () => {
    // The point of the whole thing: the session subscribes once, and its intent
    // is replayed onto whatever transport the slot is holding at the time.
    type Bus = { tick: number }
    const wire = slot<Envelope>({ own: true })
    const session = Session.over<Bus>('app', wire)
    const got: number[] = []
    session.topic('tick').listen((n) => got.push(n))
    expect(session.connected).toBe(false)

    const first = Hub.create<Bus>('app')
    const [mine, theirs] = pair<Envelope>('client', 'hub-1')
    first.serve(theirs)
    const offFirst = wire.use(mine)
    await settled()

    expect(session.connected).toBe(true)
    first.publish('tick', 1)
    await settled()
    expect(got).toEqual([1])

    // Cut the wire. The session goes down but does not end.
    offFirst()
    expect(session.connected).toBe(false)
    expect(session.closed).toBe(false)

    // Splice in a different hub. Nothing re-subscribes by hand.
    const second = Hub.create<Bus>('app')
    const [next, far] = pair<Envelope>('client', 'hub-2')
    second.serve(far)
    const offSecond = wire.use(next)
    await settled()

    expect(session.connected).toBe(true)
    expect(wire.remote).toBe('hub-2')
    second.publish('tick', 2)
    await settled()
    expect(got).toEqual([1, 2])

    // ...and a send made while unbound reaches whoever is bound next.
    const seen: number[] = []
    const watcher = second.local()
    watcher.topic('tick').listen((n) => seen.push(n))
    await settled()

    offSecond()
    expect(session.topic('tick').send(3)).toBe(true)
    expect(seen).toEqual([])

    const [third, alsoFar] = pair<Envelope>('client', 'hub-2')
    second.serve(alsoFar)
    wire.use(third)
    await settled()
    await settled()
    expect(seen).toEqual([3])

    session.close()
    first.close()
    second.close()
  })
})
