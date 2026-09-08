import { detacher, type Unsub } from '../../core/index.ts'
import { noop, report } from '../../core/internal.ts'
import type { Link } from './index.ts'

/**
 * An `Link.Source` from a subscription.
 *
 * `open` attaches and either returns its teardown or hangs everything off
 * `host.signal`. What it produces through `offer` is owned by the source: the
 * teardown closes links still open and forgets ones that closed themselves, so
 * stopping a source actually disconnects its peers rather than leaking them.
 *
 * @example
 * ```ts
 * const source = defineSource<T>((host) => {
 *   const on = (p: Port) => host.offer(link<T>(p))
 *   runtime.onConnect.addListener(on)
 *   return () => runtime.onConnect.removeListener(on)
 * })
 * ```
 */
export const defineSource =
  <S, R = S>(open: (host: Link.SourceHost<S, R>) => Unsub | void, opts: Link.SourceOptions = {}): Link.Source<S, R> =>
  (onLink) => {
    // Everything that stops the source accepting. Separate from the links it
    // produced, because those are closed only after it has stopped taking more.
    const accepting = detacher()
    const links = new Set<Link<S, R>>()

    if (opts.signal?.aborted) {
      accepting.stop()
      return noop
    }

    const stop: Unsub = () => {
      if (accepting.stopped) return
      opts.signal?.removeEventListener('abort', stop)
      accepting.stop()
      for (const one of [...links]) one.close()
      links.clear()
    }

    const ac = new AbortController()
    accepting.add(() => ac.abort())
    opts.signal?.addEventListener('abort', stop, { once: true })

    const host: Link.SourceHost<S, R> = {
      signal: ac.signal,
      fail: (err) => report(err, opts.onError),
      offer: (one) => {
        // An event already in flight when the source stopped. Closing it is the
        // only honest answer: nobody is left to serve it.
        if (accepting.stopped) {
          one.close()
          return
        }
        links.add(one)
        one.closed(() => links.delete(one))
        onLink(one)
      },
    }

    // Added rather than assigned: `open` may stop the source from inside
    // itself, and then this runs immediately instead of being held forever.
    const off = open(host)
    if (off) accepting.add(off)
    return stop
  }
