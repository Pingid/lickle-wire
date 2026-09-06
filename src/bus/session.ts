import { emitter, noop, queue, report, validateSync, type Emitter } from '../core/internal.ts'
import type { Port, Unsub } from '../index.ts'
import type { ILink } from './link/index.ts'
import * as Protocol from './protocol.ts'

/**
 * Layer 2 — the client side.
 *
 * The session owns the *desired* subscription set. The link is disposable; the
 * intent is not. Intent is replayed on every `ready`, so there is exactly one
 * code path that sends `sub`.
 *
 * The session takes a link rather than a connector. Reconnection, backoff and
 * outbound buffering are `Link.persistent`'s job; what is left here is the
 * intent set and the codec. `define(...).connect(...)` wires the two together
 * for the common case and gives back a session whose `link.state` is typed.
 */

export declare namespace Session {
  export interface MessageMeta {
    /** The receive channel. Useful when one listener serves several topics. */
    readonly channel: string
    /** Publishing peer id, or null when the hub itself published. */
    readonly from: string | null
    /** True for a value replayed on subscribe rather than live traffic. */
    readonly retained: boolean
  }

  export interface SendOptions {
    /**
     * Deliver only to this peer id, bypassing subscriptions. The id arrives as
     * `meta.from` on a received message, which is what makes request/response
     * expressible without a second channel per caller.
     */
    to?: string | undefined
  }

  export interface StreamOptions extends Port.ListenOptions {
    /** Payloads held while the consumer is slow. Oldest are dropped. Default 64. */
    buffer?: number | undefined
  }

  /**
   * A topic is a {@link Port}: hand it straight to `@lickle/wire/rpc` or
   * `@lickle/wire/replica`, no adapter needed.
   */
  export interface Topic<S, R = S, TO extends string = string, FROM extends string = string> extends Port.Port<S, R> {
    /** Channel this topic publishes to. */
    readonly to: TO
    /** Channel this topic listens on. Equal to `to` for a symmetric topic. */
    readonly from: FROM
    /**
     * False when the payload was dropped rather than handed to the transport.
     * Subscription intent survives a disconnect; a single publish only survives
     * within `Link.persistent`'s buffer and TTL, so the caller is told instead of
     * finding out never.
     */
    send(payload: S, opts?: SendOptions): boolean
    /**
     * Subscribes on the first listener, unsubscribes when the last one leaves.
     * `onClose` fires when the session ends.
     */
    listen(fn: (payload: R, meta: MessageMeta) => void, opts?: Port.ListenOptions): Unsub
    /** Detaches after the first payload. */
    once(fn: (payload: R, meta: MessageMeta) => void, opts?: Port.ListenOptions): Unsub
    /** `for await (const p of topic.stream({ signal }))`. Ends when the session does. */
    stream(opts?: StreamOptions): AsyncIterableIterator<R>
    /**
     * An addressed port: sends reach only `id`, and only payloads published by
     * `id` are delivered. One request/response conversation per peer, over a
     * channel everyone shares.
     */
    peer(id: string): Port.Port<S, R>
  }

  export type SessionEvent = 'open' | 'close' | 'error'

  export interface Options<T extends Protocol.Topics = Protocol.Topics> {
    /** Narrow inbound payloads. Rejected ones are dropped and reported. */
    validate?: Protocol.Validators<T> | undefined
    /** A throwing listener, a protocol fault, or a connector failure. */
    onError?: ((err: unknown) => void) | undefined
    /** The hub accepted us and intent has been replayed. Fires on every reconnect. */
    onOpen?: (() => void) | undefined
    /** The link went down under us. Not terminal: a persistent link comes back. */
    onClose?: (() => void) | undefined
  }
}

/** Not public: one registered topic listener. */
type Sink = (payload: unknown, meta: Session.MessageMeta) => void

export class Session<
  T extends Protocol.Topics = Protocol.Topics,
  L extends ILink<Protocol.Envelope> = ILink<Protocol.Envelope>,
> {
  #listeners = new Map<string, Set<Sink>>()
  #topics = new Map<string, Session.Topic<never, never>>()
  #stop: Unsub[] = []
  #ready = false
  #closed = false
  #id: string | null = null
  #validate: Protocol.Validators<T>
  #events: { open: Emitter<[]>; close: Emitter<[]>; fault: Emitter<[unknown]>; ended: Emitter<[]> }

  static over = <
    T extends Protocol.Topics = Protocol.Topics,
    L extends ILink<Protocol.Envelope> = ILink<Protocol.Envelope>,
  >(
    name: string,
    link: L,
    opts: Session.Options<T> = {},
  ): Session<T, L> => new Session<T, L>(name, link, opts)

  private constructor(
    /** The hub name this session speaks to. Tags every frame. */
    public readonly name: string,
    public readonly link: L,
    opts: Session.Options<T> = {},
  ) {
    this.#validate = opts.validate ?? {}
    const onError = (err: unknown) => this.#fail(err)
    this.#events = {
      open: emitter<[]>(onError),
      close: emitter<[]>(onError),
      fault: emitter<[unknown]>(),
      ended: emitter<[]>(onError),
    }
    if (opts.onOpen) this.#events.open.add(opts.onOpen)
    if (opts.onClose) this.#events.close.add(opts.onClose)
    if (opts.onError) this.#events.fault.add(opts.onError)

    // `closed` is terminal on a link — a persistent one only fires it on
    // give-up or an explicit close — so the session ends with it. A transient
    // `up: false` is `#down`: intent is kept and replayed on the next `ready`.
    this.#stop = [
      link.listen((m) => this.#recv(m)),
      link.changed((up) => {
        if (!up) this.#down()
      }),
      link.closed(() => this.close()),
    ]
    // `closed` fired synchronously if the link was already dead; `close()` ran
    // against the empty initial `#stop`, so release what was just attached.
    if (this.#closed) {
      for (const s of this.#stop) s()
      this.#stop = []
    }
  }

  /** Peer id assigned by the hub. Changes on reconnect; null while down. */
  get id(): string | null {
    return this.#id
  }

  /** The link is up and the hub has greeted us. */
  get connected(): boolean {
    return !this.#closed && this.link.up && this.#ready
  }

  /** True once the session has ended, by `close()` or its link closing. */
  get closed(): boolean {
    return this.#closed
  }

  /**
   * Subscribe to session lifecycle. Returns a teardown and accepts a signal,
   * like everything at layer 0 — the single constructor callbacks are still
   * there, but they cannot be added late or stacked.
   *
   * `'close'` is the link going down, which a persistent link recovers from;
   * `opts.onClose` is the session ending, which nothing recovers from.
   */
  on(event: Session.SessionEvent, fn: (err?: unknown) => void, opts: Port.ListenOptions = {}): Unsub {
    if (this.#closed) {
      opts.onClose?.()
      return noop
    }
    const off =
      event === 'error'
        ? this.#events.fault.add(fn, opts.signal)
        : this.#events[event].add(fn as () => void, opts.signal)
    return this.#withEnd(off, opts)
  }

  /** Resolves on the next `open`, immediately if already connected. Rejects when the session ends. */
  ready(opts: Port.ListenOptions = {}): Promise<void> {
    if (this.connected) return Promise.resolve()
    if (this.#closed) return Promise.reject(new Error(`session ${this.name} is closed`))
    return new Promise<void>((resolve, reject) => {
      if (opts.signal?.aborted) {
        reject(opts.signal.reason as Error)
        return
      }
      const ac = new AbortController()
      const settle = (fn: () => void) => {
        ac.abort()
        fn()
      }
      this.#events.open.add(() => settle(resolve), ac.signal)
      this.#events.ended.add(() => settle(() => reject(new Error(`session ${this.name} is closed`))), ac.signal)
      opts.signal?.addEventListener('abort', () => settle(() => reject(opts.signal?.reason as Error)), {
        once: true,
        signal: ac.signal,
      })
    })
  }

  /**
   * `topic('a')` is symmetric; `topic('req', 'res')` publishes and listens on
   * different channels. Identity is the pair, and topics are cached, so channel
   * names should come from a fixed set rather than being minted per tab.
   */
  topic<S extends keyof T & string, R extends keyof T & string = S>(
    to: S,
    from: R = to as unknown as R,
  ): Session.Topic<T[S], T[R], S, R> {
    const key = `${to} ${from}`
    const cached = this.#topics.get(key)
    if (cached) return cached as unknown as Session.Topic<T[S], T[R], S, R>

    const listen: Session.Topic<T[S], T[R], S, R>['listen'] = (fn, o = {}) => {
      if (this.#closed) {
        o.onClose?.()
        return noop
      }
      if (o.signal?.aborted) return noop
      // Refcount is per receive channel, shared across every topic reading it.
      const ls = this.#listeners.get(from) ?? new Set<Sink>()
      this.#listeners.set(from, ls)
      const sink: Sink = (payload, meta) => fn(payload as T[R], meta)
      ls.add(sink)
      // While down, the replay on `ready` is what carries this.
      if (ls.size === 1 && this.#ready) this.link.send(Protocol.seal(this.name, { t: 'sub', c: from }))
      let offEnd: Unsub | undefined
      let done = false
      const off = () => {
        if (done) return
        done = true
        o.signal?.removeEventListener('abort', off)
        offEnd?.()
        if (!ls.delete(sink)) return
        if (ls.size > 0) return
        this.#listeners.delete(from)
        if (this.#ready) this.link.send(Protocol.seal(this.name, { t: 'unsub', c: from }))
      }
      const onClose = o.onClose
      if (onClose) offEnd = this.#events.ended.add(() => onClose(), o.signal)
      o.signal?.addEventListener('abort', off, { once: true })
      return off
    }

    const t: Session.Topic<T[S], T[R], S, R> = {
      to,
      from,
      send: (payload, o) => {
        if (this.#closed) return false
        const frame = o?.to
          ? ({ t: 'data', c: to, d: payload, to: o.to } as const)
          : ({ t: 'data', c: to, d: payload } as const)
        return this.link.send(Protocol.seal(this.name, frame))
      },
      listen,
      once: (fn, o) => {
        const off = listen((payload, meta) => {
          off()
          fn(payload, meta)
        }, o)
        return off
      },
      stream: (o = {}) => {
        const q = queue<T[R]>({ buffer: o.buffer ?? 64, onReturn: () => off() })
        const off = listen((payload) => q.push(payload), {
          signal: o.signal,
          onClose: (err) => {
            q.end()
            o.onClose?.(err)
          },
        })
        o.signal?.addEventListener('abort', () => void q.iterator.return?.(), { once: true })
        return q.iterator
      },
      peer: (id) => ({
        send: (payload) => {
          t.send(payload, { to: id })
        },
        listen: (next, o) =>
          listen((payload, meta) => {
            if (meta.from === id) next(payload)
          }, o),
      }),
    }

    this.#topics.set(key, t as unknown as Session.Topic<never, never>)
    return t
  }

  /** Closes the link too. A session owns the link it was handed. */
  close(): void {
    if (this.#closed) return
    const wasReady = this.#ready
    this.#closed = true
    this.#ready = false
    this.#id = null
    for (const s of this.#stop) s()
    this.#stop = []
    this.#listeners.clear()
    this.#topics.clear()
    this.link.close()
    if (wasReady) this.#events.close.emit()
    this.#events.ended.emit()
    for (const e of Object.values(this.#events)) e.clear()
  }

  #withEnd(off: Unsub, opts: Port.ListenOptions): Unsub {
    const onClose = opts.onClose
    if (!onClose) return off
    const offEnd = this.#events.ended.add(() => onClose(), opts.signal)
    return () => {
      off()
      offEnd()
    }
  }

  #fail(err: unknown): void {
    if (this.#events.fault.size === 0) {
      report(err)
      return
    }
    this.#events.fault.emit(err)
  }

  #recv(m: Protocol.Envelope): void {
    const r = Protocol.unseal(this.name, m)
    if (!r.ok) {
      // A foreign frame is another hub's business. A version mismatch is ours,
      // and silence is how a stale context retries forever unnoticed.
      if (r.reason !== 'foreign') {
        this.#fail(
          new Protocol.ProtocolError(r.reason, `discarded a ${r.reason} frame from ${this.link.remote || 'the hub'}`, {
            hub: this.name,
            version: r.version,
          }),
        )
      }
      return
    }
    const f = r.frame
    switch (f.t) {
      case 'ready': {
        this.#ready = true
        this.#id = f.id
        // Replay intent. This is the whole reason the session owns the set.
        for (const [c, ls] of this.#listeners) {
          if (ls.size > 0) this.link.send(Protocol.seal(this.name, { t: 'sub', c }))
        }
        this.#events.open.emit()
        return
      }
      case 'data': {
        const ls = this.#listeners.get(f.c)
        if (!ls || ls.size === 0) return
        const check = this.#validate[f.c]
        let payload = f.d
        if (check) {
          const ok = validateSync(check, f.d)
          if (!ok.ok) {
            this.#fail(
              new Protocol.ProtocolError('payload', `rejected a payload on "${f.c}"`, {
                hub: this.name,
                channel: f.c,
                peer: f.from,
              }),
            )
            return
          }
          payload = ok.value
        }
        const meta: Session.MessageMeta = { channel: f.c, from: f.from ?? null, retained: f.r === true }
        for (const fn of [...ls]) {
          try {
            fn(payload, meta)
          } catch (err) {
            // One bad listener must not starve the rest.
            this.#fail(err)
          }
        }
        return
      }
      // Sessions do not route.
      default:
        return
    }
  }

  #down(): void {
    if (!this.#ready) return
    this.#ready = false
    this.#id = null
    this.#events.close.emit()
  }
}
