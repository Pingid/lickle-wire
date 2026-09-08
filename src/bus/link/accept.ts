import { bind, type Unsub } from '../../core/index.ts'
import { report } from '../../core/internal.ts'
import { defineSource } from './source.ts'
import type { Link } from './index.ts'

/**
 * Peers that connect themselves.
 *
 * The shape is the same on every platform: a peer mints a duplex channel, hands
 * one end towards the hub and keeps the other; ancestors relay the offer
 * without ever joining the data path. What differs is only how a channel is
 * minted and moved — `MessageChannel` and transferables in a page,
 * `runtime.connect` in an extension, a fresh socket on a server.
 *
 * So the policy lives here and the mechanism lives in the platform module:
 * which offers to admit, in what order the checks run, how a relayed hop is
 * recorded, and the rule that declining must never touch the channel.
 */

export declare namespace Accept {
  /** One inbound connection attempt, however the platform learned of it. */
  export interface Offer<C> {
    /** Peer-chosen label. Not unique. */
    readonly name: string
    /**
     * Where it came from — an origin, an extension id, a socket address. `''`
     * when the platform has no notion of one.
     */
    readonly origin: string
    /** Hops it was already relayed through, nearest first. Empty when direct. */
    readonly path: readonly string[]
    /** What the peer said about itself. Untrusted, and merged under the facts. */
    readonly claimed?: Link.Meta | undefined
    /**
     * The platform's handle on the connection: everything {@link accept}'s
     * `wrap` needs to build a link, and nothing this module looks inside.
     */
    readonly channel: C
  }

  /** What a {@link Options.filter} is shown. No `channel`: declining must not touch it. */
  export interface Details {
    readonly name: string
    readonly origin: string
    readonly path: readonly string[]
    readonly claimed?: Link.Meta | undefined
  }

  export type Origins = readonly string[] | ((origin: string) => boolean)

  export interface Options<C> extends Link.SourceOptions {
    /**
     * Origins to admit. Omitted admits every one, which is right for a platform
     * with no notion of origin and wrong for any that has one — a platform
     * module with origins always passes its own default rather than leaving
     * this open.
     */
    origins?: Origins | undefined
    /**
     * Selects the offers this acceptor handles, usually by `name`.
     *
     * Declining is never destructive: the channel is left untouched for whoever
     * else may want it, because one broadcast can reach several acceptors and
     * closing here would kill a peer a sibling just took. Rejecting a peer that
     * *is* yours is `Hub.Policy.canAccept`, which runs once the hub owns the
     * link and can safely close it.
     */
    filter?: ((details: Details) => boolean) | undefined
    /** Per-connection facts only the platform knows. Merged over what the peer claimed. */
    extract?: ((offer: Offer<C>) => Link.Meta | undefined) | undefined
  }
}

const allower = (origins?: Accept.Origins): ((origin: string) => boolean) => {
  if (origins === undefined) return () => true
  if (typeof origins === 'function') return origins
  const set = new Set(origins)
  return (origin) => set.has(origin)
}

/**
 * The allowlist and the filter, in that order, on the offer as it arrived —
 * before the hop is recorded, so a filter sees the path the peer travelled and
 * not one with this acceptor already on it.
 */
const screened = <C>(o: Accept.Offer<C>, allowed: (origin: string) => boolean, opts: Accept.Options<C>): boolean => {
  if (!allowed(o.origin)) return false
  if (!opts.filter) return true
  try {
    return opts.filter({ name: o.name, origin: o.origin, path: o.path, claimed: o.claimed }) !== false
  } catch (err) {
    // A user predicate is not allowed to take the listener down with it.
    report(err, opts.onError)
    return false
  }
}

/** Record this hop. Nearest first, so the hub reads the chain outwards. */
const hopped = (path: readonly string[], origin: string): readonly string[] => [origin, ...path]

/**
 * Turn a stream of inbound offers into a {@link Link.Source}.
 *
 * `offers` attaches to whatever the platform delivers connection attempts on
 * and calls `take` for each; `wrap` turns an admitted one into a link. The
 * source owns what it produced, so its teardown disconnects the peers it
 * accepted.
 *
 * @example
 * ```ts
 * // an extension background script
 * accept<Envelope, chrome.runtime.Port>(
 *   (take) => {
 *     const on = (p: chrome.runtime.Port) =>
 *       take({ name: p.name, origin: p.sender?.origin ?? '', path: [], channel: p })
 *     chrome.runtime.onConnect.addListener(on)
 *     return () => chrome.runtime.onConnect.removeListener(on)
 *   },
 *   (offer, meta) => chromeLink(offer.channel, { meta }),
 * )
 * ```
 */
export const accept = <T, C>(
  offers: (take: (offer: Accept.Offer<C>) => void, host: Link.SourceHost<T>) => Unsub | void,
  wrap: (offer: Accept.Offer<C>, meta: Link.Meta) => Link<T>,
  opts: Accept.Options<C> = {},
): Link.Source<T> => {
  const allowed = allower(opts.origins)
  return defineSource<T>((host) => {
    const take = (o: Accept.Offer<C>) => {
      if (!screened(o, allowed, opts)) return
      let one: Link<T>
      try {
        one = wrap(o, {
          ...o.claimed,
          origin: o.origin,
          path: hopped(o.path, o.origin),
          ...opts.extract?.(o),
        })
      } catch (err) {
        // Wrapping failed, so there is no link to offer and nothing to close.
        host.fail(err)
        return
      }
      host.offer(one)
    }
    return offers(take, host)
  }, opts)
}

/**
 * Relay offers one hop closer to the hub, recording where each came from, so
 * the hub's `peer.meta.path` names the whole chain and a policy can act on it.
 *
 * Never produces a link — a relay is not in the data path — so this returns a
 * plain teardown rather than a source.
 *
 * Only one handler per stream may claim a given offer: two that both move the
 * same channel will fight over it. Partition with `filter`.
 */
export const relay = <C>(
  offers: (take: (offer: Accept.Offer<C>) => void) => Unsub,
  forward: (offer: Accept.Offer<C>) => void,
  opts: Omit<Accept.Options<C>, 'extract'> = {},
): Unsub => {
  const allowed = allower(opts.origins)
  return bind(
    () =>
      offers((o) => {
        if (!screened(o, allowed, opts)) return
        try {
          forward({ ...o, path: hopped(o.path, o.origin) })
        } catch (err) {
          // Reported rather than thrown out of a platform callback, where
          // nothing can catch it.
          report(err, opts.onError)
        }
      }),
    opts.signal,
  )
}
