import { baseLink, type BaseLink } from './base.ts'
import { report } from '../../core/internal.ts'
import type { ILink } from './index.ts'

export type Pair<T> = readonly [ILink<T>, ILink<T>]

export declare namespace Pair {
  export interface Options {
    /**
     * Payloads are structured-cloned by default, because that is what every real
     * transport does. A pair that passed references let a test pass while
     * production mutated one payload across N receivers. `false` for speed when
     * the payloads are known to be immutable.
     */
    clone?: boolean | (<V>(value: V) => V) | undefined
    metaA?: ILink.Meta | undefined
    metaB?: ILink.Meta | undefined
    /** Inbound buffered before the first `listen`. Matches the adapters. */
    pending?: number | undefined
    onError?: ((err: unknown) => void) | undefined
  }
}

/**
 * Two cross-wired ends in the same context. This is what makes a hub's own
 * process a first-class peer — `chrome.runtime.connect` never delivers to the
 * caller, so a worker cannot reach its own `onConnect`.
 *
 * Each argument names its own end; each end's `remote` is the other's name.
 * Delivery is deferred to a microtask and closing is synchronous, because that
 * is what the real transports do. Anywhere this is gentler than a real
 * transport, a test passes and production fails.
 */
export const pair = <T>(nameA = 'A', nameB = 'B', opts: Pair.Options = {}): Pair<T> => {
  const clone = cloner(opts.clone)
  const metaA = opts.metaA ?? {}
  const metaB = opts.metaB ?? {}
  let open = true

  const core = (i: 0 | 1): BaseLink<T> =>
    baseLink<T>({
      describe: () => ({ remote: i === 0 ? nameB : nameA, meta: i === 0 ? metaB : metaA }),
      pending: opts.pending,
      onError: opts.onError,
    })

  const cores = [core(0), core(1)] as const

  const shut = () => {
    if (!open) return
    open = false
    for (const c of cores) c.shut()
  }

  const end = (i: 0 | 1): ILink<T> =>
    cores[i].expose((msg) => {
      if (!open) return false
      let copy: T
      try {
        copy = clone(msg)
      } catch (err) {
        // A payload that would fail structured clone on a real transport. Fail
        // here too, rather than letting it work in tests only.
        report(err, opts.onError)
        return false
      }
      queueMicrotask(() => {
        if (open) cores[(1 - i) as 0 | 1].deliver(copy)
      })
      return true
    }, shut)

  return [end(0), end(1)] as const
}

const identity = <V>(v: V): V => v

const structural = <V>(v: V): V =>
  typeof structuredClone === 'function' ? structuredClone(v) : (JSON.parse(JSON.stringify(v)) as V)

const cloner = (c: Pair.Options['clone']): (<V>(v: V) => V) =>
  c === false ? identity : typeof c === 'function' ? c : structural
