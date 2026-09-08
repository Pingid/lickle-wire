import { describe, expect, it, vi } from 'vitest'
import type { Port } from '../../core/index.ts'
import { accept, relay, type Accept } from './accept.ts'
import { asLink, defineLink } from './base.ts'
import type { Link } from './index.ts'

/** A channel handle standing in for a transferred port. */
type Chan = { id: number; closed: boolean }

const chan = (id: number): Chan => ({ id, closed: false })

const wrap = (offer: Accept.Offer<Chan>, meta: Link.Meta): Link<string> =>
  defineLink<string>(() => ({ send: () => true, release: () => (offer.channel.closed = true) }), {
    remote: offer.name,
    meta,
  })

/** Drives offers by hand, so the policy is tested without a platform. */
const rig = (opts: Accept.Options<Chan> = {}) => {
  let take!: (o: Accept.Offer<Chan>) => void
  const off = vi.fn()
  const got: Link<string>[] = []
  const stop = accept<string, Chan>(
    (t) => {
      take = t
      return off
    },
    wrap,
    opts,
  )((one) => got.push(one))
  return { take: take!, stop, got, off }
}

const offer = (o: Partial<Accept.Offer<Chan>> = {}): Accept.Offer<Chan> => ({
  name: 'app',
  origin: 'https://a.example',
  path: [],
  channel: chan(1),
  ...o,
})

describe('accept', () => {
  it('records the hop in meta, nearest first, and keeps the peer claims under the facts', () => {
    const { take, got } = rig()
    take(offer({ claimed: { role: 'panel', origin: 'https://lies.example' }, path: ['https://mid.example'] }))

    expect(got).toHaveLength(1)
    const one = got[0] as Link<string>
    expect(one.remote).toBe('app')
    expect(one.meta['role']).toBe('panel')
    // A peer states `origin` about itself; the acceptor knows better.
    expect(one.meta.origin).toBe('https://a.example')
    expect(one.meta.path).toEqual(['https://a.example', 'https://mid.example'])
  })

  it('admits only allowed origins, and admits everything when none are named', () => {
    const named = rig({ origins: ['https://a.example'] })
    named.take(offer({ origin: 'https://a.example' }))
    named.take(offer({ origin: 'https://b.example' }))
    expect(named.got).toHaveLength(1)

    const open = rig()
    open.take(offer({ origin: 'https://anywhere.example' }))
    expect(open.got).toHaveLength(1)

    const fn = rig({ origins: (o) => o.endsWith('.trusted') })
    fn.take(offer({ origin: 'x.trusted' }))
    fn.take(offer({ origin: 'x.evil' }))
    expect(fn.got).toHaveLength(1)
  })

  it('shows the filter the path the peer travelled, not one with this hop on it', () => {
    const seen: Accept.Details[] = []
    const { take, got } = rig({ filter: (d) => (seen.push(d), d.name === 'app') })
    take(offer({ path: ['https://mid.example'] }))
    take(offer({ name: 'other' }))

    expect(got).toHaveLength(1)
    expect(seen[0]?.path).toEqual(['https://mid.example'])
    // ...while the link that was admitted has it recorded.
    expect((got[0] as Link<string>).meta.path).toEqual(['https://a.example', 'https://mid.example'])
  })

  it('never touches a channel it declined, because a sibling acceptor may want it', () => {
    const mine = chan(1)
    const { take, got } = rig({ filter: (d) => d.name === 'mine' })
    take(offer({ name: 'theirs', channel: mine }))
    expect(got).toHaveLength(0)
    expect(mine.closed).toBe(false)
  })

  it('survives a throwing filter without taking the listener down', () => {
    const onError = vi.fn()
    const { take, got } = rig({
      onError,
      filter: (d) => {
        if (d.name === 'bad') throw new Error('boom')
        return true
      },
    })
    take(offer({ name: 'bad' }))
    expect(onError).toHaveBeenCalledTimes(1)
    take(offer({ name: 'good' }))
    expect(got).toHaveLength(1)
  })

  it('merges extract over everything, and reports a wrap that throws', () => {
    const withExtract = rig({ extract: (o) => ({ id: o.channel.id, origin: 'forced' }) })
    withExtract.take(offer({ channel: chan(7) }))
    expect((withExtract.got[0] as Link<string>).meta['id']).toBe(7)
    expect((withExtract.got[0] as Link<string>).meta.origin).toBe('forced')

    const onError = vi.fn()
    const boom = accept<string, Chan>(
      (t) => void (t(offer()) as unknown),
      () => {
        throw new Error('cannot wrap')
      },
      { onError },
    )(() => {})
    expect(onError).toHaveBeenCalledTimes(1)
    boom()
  })

  it('owns what it admitted: stopping disconnects the peers and the listener', () => {
    const { take, stop, got, off } = rig()
    const a = chan(1)
    const b = chan(2)
    take(offer({ channel: a }))
    take(offer({ channel: b }))
    expect(got).toHaveLength(2)

    stop()
    expect(off).toHaveBeenCalledTimes(1)
    expect([a.closed, b.closed]).toEqual([true, true])
  })
})

describe('relay', () => {
  const rrig = (opts: Omit<Accept.Options<Chan>, 'extract'> = {}) => {
    let take!: (o: Accept.Offer<Chan>) => void
    const off = vi.fn()
    const sent: Accept.Offer<Chan>[] = []
    const stop = relay<Chan>(
      (t) => {
        take = t
        return off
      },
      (o) => sent.push(o),
      opts,
    )
    return { take: take!, stop, sent, off }
  }

  it('forwards with this hop prepended, so the hub reads the whole chain', () => {
    const { take, sent } = rrig()
    take(offer({ origin: 'https://frame.example', path: ['https://inner.example'] }))
    expect(sent[0]?.path).toEqual(['https://frame.example', 'https://inner.example'])
    expect(sent[0]?.name).toBe('app')
  })

  it('applies origins and filter like accept, and never produces a link', () => {
    const { take, sent } = rrig({ origins: ['https://a.example'], filter: (d) => d.name === 'app' })
    take(offer({ origin: 'https://b.example' }))
    take(offer({ name: 'other' }))
    take(offer())
    expect(sent).toHaveLength(1)
  })

  it('reports a forward that throws rather than unwinding the platform callback', () => {
    const onError = vi.fn()
    let take!: (o: Accept.Offer<Chan>) => void
    relay<Chan>(
      (t) => {
        take = t
        return () => {}
      },
      () => {
        throw new Error('already started')
      },
      { onError },
    )
    expect(() => take(offer())).not.toThrow()
    expect(onError).toHaveBeenCalledTimes(1)
  })

  it('detaches on teardown and on signal abort', () => {
    const { stop, off } = rrig()
    stop()
    stop()
    expect(off).toHaveBeenCalledTimes(1)

    const ac = new AbortController()
    const second = rrig({ signal: ac.signal })
    ac.abort()
    expect(second.off).toHaveBeenCalledTimes(1)
  })
})

describe('asLink', () => {
  /** A hand-driven Port, the thing `asLink` is for. */
  const port = () => {
    const fns = new Set<(v: string) => void>()
    const closes = new Set<(err?: unknown) => void>()
    const sent: string[] = []
    let closed = false
    return {
      sent,
      deliver: (v: string) => fns.forEach((f) => f(v)),
      end: (err?: unknown) => {
        closed = true
        for (const c of [...closes]) c(err)
      },
      port: {
        send: (v: string) => void sent.push(v),
        listen: (next: (v: string) => void, o = {}) => {
          if (closed) {
            o.onClose?.(undefined)
            return () => {}
          }
          fns.add(next)
          if (o.onClose) closes.add(o.onClose)
          return () => fns.delete(next)
        },
      } satisfies Port<string, string>,
    }
  }

  it('carries messages both ways and buffers inbound like any link', () => {
    const p = port()
    const one = asLink(p.port, { remote: 'server' })
    expect(one.remote).toBe('server')
    expect(one.up).toBe(true)

    // Delivered before anyone listens: a link holds it, a bare Port would not.
    p.deliver('early')
    const got = vi.fn()
    one.listen(got)
    expect(got).toHaveBeenCalledWith('early')

    expect(one.send('out')).toBe(true)
    expect(p.sent).toEqual(['out'])
  })

  it("turns the port's terminal onClose into closed", () => {
    const p = port()
    const one = asLink(p.port)
    const closed = vi.fn()
    one.closed(closed)

    p.end()
    expect(closed).toHaveBeenCalledTimes(1)
    expect(one.up).toBe(false)
    expect(one.send('x')).toBe(false)
  })

  it('reports an error passed with the close before shutting', () => {
    const onError = vi.fn()
    const p = port()
    const one = asLink(p.port, { onError })
    p.end(new Error('socket reset'))
    expect(onError).toHaveBeenCalledTimes(1)
    expect(one.up).toBe(false)
  })

  it('is closed on arrival when the port already was', () => {
    const p = port()
    p.end()
    const one = asLink(p.port)
    expect(one.up).toBe(false)
    const closed = vi.fn()
    one.closed(closed)
    expect(closed).toHaveBeenCalledTimes(1)
  })
})
