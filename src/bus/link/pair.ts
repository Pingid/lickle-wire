import { defineLink } from './base.ts'
import { report } from '../../core/internal.ts'
import type { Link } from './index.ts'

export type Pair<T> = readonly [Link<T>, Link<T>]

export declare namespace Pair {
  export interface Options {
    /**
     * Payloads are structured-cloned by default, because that is what every real
     * transport does. A pair that passed references let a test pass while
     * production mutated one payload across N receivers. `false` for speed when
     * the payloads are known to be immutable.
     */
    clone?: boolean | (<V>(value: V) => V) | undefined
    metaA?: Link.Meta | undefined
    metaB?: Link.Meta | undefined
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

  // Each end delivers into the other's host, so they are collected as they are
  // built rather than passed in: the first end exists before the second does.
  const hosts: Link.Host<T>[] = []

  const shut = () => {
    if (!open) return
    open = false
    for (const h of hosts) h.shut()
  }

  const end = (i: 0 | 1): Link<T> =>
    defineLink<T>(
      (host) => {
        hosts[i] = host
        return {
          send: (msg) => {
            if (!open) return false
            let copy: T
            try {
              copy = clone(msg)
            } catch (err) {
              // A payload that would fail structured clone on a real transport.
              // Fail here too, rather than letting it work in tests only.
              report(err, opts.onError)
              return false
            }
            queueMicrotask(() => {
              if (open) hosts[(1 - i) as 0 | 1]?.deliver(copy)
            })
            return true
          },
          close: shut,
        }
      },
      {
        remote: i === 0 ? nameB : nameA,
        meta: i === 0 ? metaB : metaA,
        pending: opts.pending,
        onError: opts.onError,
      },
    )

  return [end(0), end(1)] as const
}

const identity = <V>(v: V): V => v

const structural = <V>(v: V): V =>
  typeof structuredClone === 'function' ? structuredClone(v) : (JSON.parse(JSON.stringify(v)) as V)

const cloner = (c: Pair.Options['clone']): (<V>(v: V) => V) =>
  c === false ? identity : typeof c === 'function' ? c : structural
