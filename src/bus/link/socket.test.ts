import { describe, expect, it, vi } from 'vitest'
import type { Link } from './index.ts'
import { socketSource, type Socket } from './socket.ts'

/** A hand-driven stand-in for a platform socket: Bun's, `ws`'s, the browser's. */
const sock = (): Socket<string> & { sent: string[]; closed: boolean } => {
  const sent: string[] = []
  let closed = false
  return {
    sent,
    get closed() {
      return closed
    },
    send: (data: string) => sent.push(data),
    close: () => (closed = true),
  }
}

/** Serves the source by hand, so behaviour is tested without a `Hub`. */
const serve = <M>(source: Link.Source<M>) => {
  const got: Link<M>[] = []
  const stop = source((one) => got.push(one))
  return { got, stop }
}

describe('socketSource', () => {
  it('offers a link on open, wired to the socket', () => {
    const sockets = socketSource<Socket<string>, string>()
    const { got } = serve(sockets.source)
    const a = sock()

    sockets.open(a)
    expect(got).toHaveLength(1)

    got[0]?.send('hi')
    expect(a.sent).toEqual([JSON.stringify('hi')])
  })

  it('decodes inbound frames and delivers them to the link', () => {
    const sockets = socketSource<Socket<string>, string>()
    const { got } = serve(sockets.source)
    const a = sock()
    sockets.open(a)

    const heard = vi.fn()
    got[0]?.listen(heard)
    sockets.message(a, JSON.stringify('from-a'))
    expect(heard).toHaveBeenCalledWith('from-a')
  })

  it('closes the socket when the link does, and shuts the link when the socket does', () => {
    const sockets = socketSource<Socket<string>, string>()
    const { got } = serve(sockets.source)
    const a = sock()
    sockets.open(a)

    got[0]?.close()
    expect(a.closed).toBe(true)

    const b = sock()
    sockets.open(b)
    const ended = vi.fn()
    got[1]?.closed(ended)
    sockets.close(b)
    expect(ended).toHaveBeenCalledTimes(1)
  })

  it('keeps sockets independent: closing one leaves the other listening', () => {
    const sockets = socketSource<Socket<string>, string>()
    const { got } = serve(sockets.source)
    const a = sock()
    const b = sock()
    sockets.open(a)
    sockets.open(b)

    sockets.close(a)
    expect(got[0]?.up).toBe(false)
    expect(got[1]?.up).toBe(true)

    got[1]?.send('still here')
    expect(b.sent).toEqual([JSON.stringify('still here')])
  })

  it('ignores a message or close for a socket it never opened', () => {
    const sockets = socketSource<Socket<string>, string>()
    serve(sockets.source)
    const stray = sock()
    expect(() => sockets.message(stray, 'x')).not.toThrow()
    expect(() => sockets.close(stray)).not.toThrow()
  })

  it('reports an error for a socket it never opened, rather than swallowing it', () => {
    const onError = vi.fn()
    const sockets = socketSource<Socket<string>, string>({ onError })
    serve(sockets.source)

    const boom = new Error('failed upgrade')
    sockets.error(sock(), boom)
    expect(onError).toHaveBeenCalledWith(boom)
  })

  it('labels the link from remote', () => {
    const sockets = socketSource<Socket<string>, string>({ remote: (s) => `peer-${(s as { id?: number }).id}` })
    const { got } = serve(sockets.source)
    sockets.open(Object.assign(sock(), { id: 7 }))
    expect(got[0]?.remote).toBe('peer-7')
  })

  it('closes the socket when meta throws, rather than leaving it unserved', () => {
    const onError = vi.fn()
    const boom = new Error('bad meta')
    const sockets = socketSource<Socket<string>, string>({
      meta: () => {
        throw boom
      },
      onError,
    })
    const { got } = serve(sockets.source)
    const a = sock()

    sockets.open(a)
    expect(got).toHaveLength(0)
    expect(onError).toHaveBeenCalledWith(boom)
    expect(a.closed).toBe(true)
  })

  it('keeps the live link routed when a duplicate open is closed', () => {
    const sockets = socketSource<Socket<string>, string>()
    const { got } = serve(sockets.source)
    const a = sock()
    sockets.open(a)
    sockets.open(a)

    const heard = vi.fn()
    got[1]?.listen(heard)
    got[0]?.close()

    sockets.message(a, JSON.stringify('hello'))
    expect(heard).toHaveBeenCalledWith('hello')
  })

  it('reports a fault raised through error() without closing the link', () => {
    const onError = vi.fn()
    const sockets = socketSource<Socket<string>, string>({ onError })
    const { got } = serve(sockets.source)
    const a = sock()
    sockets.open(a)

    const boom = new Error('socket fault')
    sockets.error(a, boom)
    expect(onError).toHaveBeenCalledWith(boom)
    expect(got[0]?.up).toBe(true)
  })

  it('reports a decode failure rather than losing the socket', () => {
    const onError = vi.fn()
    const sockets = socketSource<Socket<string>, string>({ onError })
    serve(sockets.source)
    const a = sock()
    sockets.open(a)

    sockets.message(a, '{not json')
    expect(onError).toHaveBeenCalledTimes(1)
  })

  it('honours custom encode and decode', () => {
    const sockets = socketSource<Socket<string>, number>({
      encode: (n) => `n:${n}`,
      decode: (d) => Number(String(d).slice(2)),
    })
    const { got } = serve(sockets.source)
    const a = sock()
    sockets.open(a)

    got[0]?.send(3)
    expect(a.sent).toEqual(['n:3'])

    const heard = vi.fn()
    got[0]?.listen(heard)
    sockets.message(a, 'n:9')
    expect(heard).toHaveBeenCalledWith(9)
  })

  it('passes the socket to meta', () => {
    const sockets = socketSource<Socket<string>, string>({ meta: (s) => ({ id: (s as { id?: number }).id }) })
    const { got } = serve(sockets.source)
    const a = Object.assign(sock(), { id: 42 })
    sockets.open(a)
    expect(got[0]?.meta['id']).toBe(42)
  })

  it('stops taking new sockets once the source is stopped, and closes what it holds', () => {
    const sockets = socketSource<Socket<string>, string>()
    const { got, stop } = serve(sockets.source)
    const a = sock()
    sockets.open(a)

    stop()
    expect(a.closed).toBe(true)

    const b = sock()
    expect(() => sockets.open(b)).not.toThrow()
    expect(got).toHaveLength(1)
  })
})
