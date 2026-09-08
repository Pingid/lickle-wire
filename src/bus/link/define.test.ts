import { describe, expect, it, vi } from 'vitest'
import { detacher } from '../../core/index.ts'
import { defineLink } from './base.ts'
import { defineSource } from './source.ts'
import type { Link } from './index.ts'

/** A link over nothing, with the host kept so a test can drive the transport side. */
const driven = <T = unknown>(o: Link.Options<T> = {}, t: Partial<Link.Transport<T>> = {}) => {
  const sent: T[] = []
  let host!: Link.Host<T>
  const link = defineLink<T>((h) => {
    host = h
    return {
      send: (m) => {
        sent.push(m)
        return true
      },
      ...t,
    }
  }, o)
  return { link, host: host!, sent }
}

describe('defineLink', () => {
  it('runs release and reports closed when open shuts synchronously', () => {
    const release = vi.fn()
    const closed = vi.fn()
    const link = defineLink((host) => {
      // A transport handed over already dead: the disconnect event fired before
      // anyone got to subscribe.
      host.shut()
      return { send: () => true, release }
    })

    expect(release).toHaveBeenCalledTimes(1)
    expect(link.up).toBe(false)
    expect(link.send('x')).toBe(false)

    link.closed(closed)
    expect(closed).toHaveBeenCalledTimes(1)
  })

  it('propagates a throw from open, so a connector failure is a failed attempt', () => {
    expect(() =>
      defineLink(() => {
        throw new Error('no transport')
      }),
    ).toThrow('no transport')
  })

  it('does not drain the pending buffer into an already-aborted listener', () => {
    const { link, host } = driven()
    host.deliver('held')

    const ac = new AbortController()
    ac.abort()
    const got = vi.fn()
    link.listen(got, { signal: ac.signal })
    expect(got).not.toHaveBeenCalled()

    // The backlog is still there for a listener that actually subscribed.
    const later = vi.fn()
    link.listen(later)
    expect(later).toHaveBeenCalledWith('held')
  })

  it('reports up from signal alone, so the getter and changed cannot disagree', () => {
    const seen: boolean[] = []
    const { link, host } = driven({ up: false })
    link.changed((up) => seen.push(up))

    expect(link.up).toBe(false)
    host.signal(true)
    expect(link.up).toBe(true)
    host.signal(true)
    host.signal(false)
    expect(link.up).toBe(false)
    expect(seen).toEqual([true, false])
  })

  it('reads remote and meta through a thunk, so a reconnecting far end tracks', () => {
    let remote = 'a'
    const { link } = driven({ remote: () => remote, meta: () => ({ origin: remote }) })
    expect(link.remote).toBe('a')
    remote = 'b'
    expect(link.remote).toBe('b')
    expect(link.meta.origin).toBe('b')
  })

  it('says goodbye through the transport before shutting on close', () => {
    const order: string[] = []
    const link = defineLink(() => ({
      send: () => true,
      close: () => order.push('bye'),
      release: () => order.push('release'),
    }))
    link.closed(() => order.push('closed'))
    link.close()
    expect(order).toEqual(['bye', 'release', 'closed'])
    link.close()
    expect(order).toEqual(['bye', 'release', 'closed'])
  })

  it('routes a survivable fault to onError without ending the link', () => {
    const onError = vi.fn()
    const { link, host } = driven({ onError })
    host.fail(new Error('bad payload'))
    expect(onError).toHaveBeenCalledTimes(1)
    expect(link.up).toBe(true)
  })
})

describe('detacher', () => {
  it('runs a teardown added after stop, rather than holding it forever', () => {
    const stops = detacher()
    const early = vi.fn()
    stops.add(early)
    stops.stop()
    expect(early).toHaveBeenCalledTimes(1)
    expect(stops.stopped).toBe(true)

    const late = vi.fn()
    stops.add(late)
    expect(late).toHaveBeenCalledTimes(1)

    stops.stop()
    expect(early).toHaveBeenCalledTimes(1)
  })
})

describe('defineSource', () => {
  const dead = () => defineLink(() => ({ send: () => true }))

  it('closes what it produced when it stops', () => {
    const made: Link<unknown>[] = []
    let offer!: (l: Link<unknown>) => void
    const source = defineSource<unknown>((host) => {
      offer = host.offer
    })

    const got: Link<unknown>[] = []
    const stop = source((l) => got.push(l))

    for (const _ of [0, 1]) {
      const l = dead()
      made.push(l)
      offer(l)
    }
    expect(got).toHaveLength(2)
    expect(made.every((l) => l.up)).toBe(true)

    stop()
    expect(made.every((l) => l.up)).toBe(false)
  })

  it('forgets a link that closed itself, and closes one offered after it stopped', () => {
    let host!: Link.SourceHost<unknown>
    const source = defineSource<unknown>((h) => {
      host = h
    })
    const stop = source(() => {})

    const gone = dead()
    host.offer(gone)
    gone.close()

    stop()

    // Offered into a stopped source: closed rather than orphaned.
    const late = dead()
    host.offer(late)
    expect(late.up).toBe(false)
  })

  it('aborts host.signal and runs open teardown on stop', () => {
    const off = vi.fn()
    let aborted = false
    const source = defineSource<unknown>((host) => {
      host.signal.addEventListener('abort', () => (aborted = true))
      return off
    })
    const stop = source(() => {})
    expect(aborted).toBe(false)
    stop()
    expect(aborted).toBe(true)
    expect(off).toHaveBeenCalledTimes(1)
    stop()
    expect(off).toHaveBeenCalledTimes(1)
  })

  it('runs open teardown immediately when the source stopped from inside open', () => {
    const off = vi.fn()
    const ac = new AbortController()
    // The source is over before `open` has handed back the teardown that would
    // normally be held — the detacher runs it on arrival instead of leaking it.
    const stop = defineSource<unknown>(
      () => {
        ac.abort()
        return off
      },
      { signal: ac.signal },
    )(() => {})

    expect(off).toHaveBeenCalledTimes(1)
    stop()
    expect(off).toHaveBeenCalledTimes(1)
  })

  it('never opens when the signal is already aborted', () => {
    const open = vi.fn()
    const ac = new AbortController()
    ac.abort()
    const stop = defineSource<unknown>(open, { signal: ac.signal })(() => {})
    expect(open).not.toHaveBeenCalled()
    stop()
  })

  it('stops when its signal aborts', () => {
    const off = vi.fn()
    const ac = new AbortController()
    const made = dead()
    let host!: Link.SourceHost<unknown>
    defineSource<unknown>(
      (h) => {
        host = h
        return off
      },
      { signal: ac.signal },
    )(() => {})

    host.offer(made)
    ac.abort()
    expect(off).toHaveBeenCalledTimes(1)
    expect(made.up).toBe(false)
  })
})
