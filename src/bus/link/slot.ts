import { detacher, type Clock, type Unsub } from '../../core/index.ts'
import { attempt, noop } from '../../core/internal.ts'
import { defineLink } from './base.ts'
import { outbox } from './outbox.ts'
import type { Link } from './index.ts'

/**
 * A link whose transport is swapped by hand.
 *
 * The point is the stable identity: a `Session`, a `Hub` or an rpc side holds
 * the slot and subscribes once, and every `listen`, `changed` and `closed` it
 * registered survives any number of binds. Only the slot's own subscription to
 * the inner link is torn down and rebuilt, which is the bookkeeping this exists
 * to own — done by hand it is where the listener leaks and the stale-callback
 * bugs live.
 *
 * {@link persistent} is this with a retry policy driving the binds. Reach for a
 * slot when the schedule is not a backoff curve: a peer the user picks from a
 * list, a worker swapped on hot reload, a test that wants to cut the wire and
 * splice it back.
 */

export interface Slot<T extends Link<any, any>> extends Link<Link.Sending<T>, Link.Receiving<T>> {
  /**
   * Attach a transport, and get back the teardown that detaches it — the same
   * shape as every other subscription here, so a slot hangs off an
   * `AbortSignal` or a `detacher` like anything else.
   *
   * Calling this again swaps: the current transport is detached first. The
   * teardown of a transport that has since been replaced is inert, so a caller
   * never has to ask whether its link is still the current one, and detaching
   * late cannot cut someone else's wire.
   *
   * A closed slot takes nothing and hands back a teardown that does nothing.
   * The link is closed if this slot `own`s it and left alone otherwise, because
   * a slot that is over must not strand what it was handed.
   */
  use(inner: T): Unsub
  /** The transport in use, or null. */
  readonly inner: T | null
}

export declare namespace Slot {
  export interface Options<T extends Link<any, any>> {
    /** Reported while unbound. A bound slot reports the inner's own. */
    remote?: string | undefined
    /** Reported while unbound. A bound slot reports the inner's own. */
    meta?: Link.Meta | undefined
    /**
     * Close the transport when it is detached and when the slot closes. Off by
     * default: you passed it to `use`, so you are assumed to still hold it. On
     * when the slot is the only handle to it.
     */
    own?: boolean | undefined
    /**
     * Outbound messages held while the slot is down. Overflow is refused at the
     * call site: `send` returns false and `onDrop` fires. 0 never buffers.
     */
    buffer?: number | undefined
    /** ms a held message may wait before it is dropped. 0 is forever. */
    ttl?: number | undefined
    /** Which end of a full buffer loses. Defaults to refusing the new message. */
    overflow?: 'oldest' | 'newest' | undefined
    /** Inbound held before the first `listen`. Covers a late-attached session. */
    pending?: number | undefined
    clock?: Clock | undefined
    /**
     * The transport in use changed, `null` when it went away — including when
     * one closed itself, which detaches the slot rather than ending it.
     */
    onInner?: ((inner: T | null) => void) | undefined
    onError?: ((err: unknown) => void) | undefined
    /** A message that will never be delivered. */
    onDrop?: ((msg: Link.Receiving<T>) => void) | undefined
  }
}

export const slot = <T extends Link<any, any>>(opts: Slot.Options<T> = {}): Slot<T> => {
  const own = opts.own ?? false
  const held = outbox<Link.Sending<T>>({ ...opts, onDrop: opts.onDrop })

  let inner: T | null = null
  let stops = detacher()
  /**
   * Names the current binding. A transport may fire `closed` from inside our
   * own `use`, its queued events may arrive after we let it go, and a teardown
   * may be called long after something else took its place — all three are the
   * same check.
   */
  let epoch = 0

  // Assigned by `defineLink` before anything below can run.
  let host!: Link.Host<Link.Sending<T>>

  const announce = (next: T | null) => attempt(() => opts.onInner?.(next), undefined, opts.onError)

  /** Let go of the current transport. `dispose` is false when it closed itself. */
  const release = (dispose: boolean): void => {
    const was = inner
    if (!was) return
    epoch += 1
    inner = null
    stops.stop()
    host.signal(false)
    announce(null)
    if (dispose && own) was.close()
  }

  /** Returns the token this binding is identified by, for its teardown. */
  const attach = (next: T): number => {
    const token = ++epoch
    inner = next
    stops = detacher()
    // Announced before subscribing, so a link that is already closed reads as
    // an attach followed by a detach rather than a detach out of nowhere.
    announce(next)

    stops.add(next.listen((msg: Link.Receiving<T>) => token === epoch && host.deliver(msg)))
    stops.add(
      next.changed((up) => {
        if (token !== epoch) return
        host.signal(up)
        if (up) held.drain((msg: Link.Sending<T>) => next.send(msg))
      }),
    )
    // Fires synchronously when the inner is already dead, which is why
    // everything above is token-guarded and `release` is safe to reach here.
    stops.add(next.closed(() => token === epoch && release(false)))

    if (token !== epoch || !next.up) return token
    host.signal(true)
    held.drain((msg: Link.Sending<T>) => next.send(msg))
    return token
  }

  const link = defineLink<Link.Sending<T>, Link.Receiving<T>>(
    (h) => {
      host = h
      return {
        send: (msg: Link.Sending<T>) => (inner && inner.up ? inner.send(msg) : held.hold(msg)),
        close: () => release(true),
        // Anything still waiting will never be delivered.
        release: () => held.clear(),
      }
    },
    {
      // Thunks: the far end changes under this link on every bind.
      remote: () => inner?.remote ?? opts.remote ?? '',
      meta: () => inner?.meta ?? opts.meta ?? {},
      pending: opts.pending,
      // Unbound is alive and down, so the first bind reads as a transition.
      up: false,
      onError: opts.onError,
      onDrop: opts.onDrop,
    },
  )

  return {
    ...link,
    get remote() {
      return link.remote
    },
    get meta() {
      return link.meta
    },
    get up() {
      return link.up
    },
    get inner() {
      return inner
    },
    use(next) {
      if (!host.alive) {
        // The slot is over. Do not strand a link that was handed to it.
        if (own) next.close()
        return noop
      }
      // Re-using what is already in use mints a second teardown for the same
      // binding rather than tearing it down and building it again.
      let token = epoch
      if (next !== inner) {
        release(true)
        token = attach(next)
      }
      let done = false
      return () => {
        if (done) return
        done = true
        // Inert once something else has taken its place.
        if (token === epoch) release(true)
      }
    },
  }
}
