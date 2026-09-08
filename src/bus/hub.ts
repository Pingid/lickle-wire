import { attempt, emitter, isPromise, noop, report, validateSync, type Emitter } from '../core/internal.ts'
import { detacher, type Detacher } from '../core/index.ts'
import type { ListenOptions, Unsub } from '../index.ts'
import { pair, type Link } from './link/index.ts'
import * as Protocol from './protocol.ts'
import { Session } from './session.ts'

/**
 * Layer 3 — routing.
 *
 * The hub fans a `data` frame out to every peer subscribed to that channel,
 * except the sender, and delivers an addressed frame to exactly one peer.
 * Policy can veto; it cannot re-route. Anything fancier composes on top —
 * return `false` from `canPublish` and call `peer.send` or `hub.publish`.
 *

 * This file imports `session`, never the reverse.
 */

export declare namespace Hub {
  export interface Peer<T extends Protocol.Topics> {
    readonly id: string
    readonly name: string
    readonly meta: Link.Meta
    /**
     * Snapshot of the peer's subscriptions at `rev`. A new object is minted on
     * every change rather than mutated in place, so identity comparison is a
     * valid change test — which is what memoised consumers actually do.
     */
    readonly channels: ReadonlySet<keyof T & string>
    /** Increments whenever this peer's subscriptions change. */
    readonly rev: number
    send<K extends keyof T & string>(channel: K, payload: T[K]): boolean
    close(): void
  }

  /**
   * Every hook is a predicate named for what it permits, so a `Policy` never
   * reads as if it does the thing it gates. Returning false from `canAccept`
   * closes the link: a peer that is being ignored should find out.
   *
   * That is safe because `accept` takes ownership of the link. Deciding whether a
   * connection belongs to this hub at all is the source's job, and a source
   * declines by ignoring — it must never close a port a sibling hub may want.
   *
   */
  export interface Policy<T extends Protocol.Topics> {
    /**
     * May be async: inbound frames are held and the greeting is withheld until it
     * settles, so a token can actually be verified here. A rejection or a throw
     * is a refusal.
     */
    canAccept?(peer: Peer<T>): boolean | Promise<boolean>
    canSubscribe?(peer: Peer<T>, channel: keyof T & string): boolean
    /** Drop or audit inbound data. Return false to swallow it. */
    canPublish?<K extends keyof T & string>(peer: Peer<T>, channel: K, payload: T[K]): boolean
    /** Gate addressed delivery. Defaults to allowed when a peer may publish. */
    canAddress?(from: Peer<T>, to: Peer<T>, channel: keyof T & string): boolean
  }

  export interface PublishOptions {
    /** Deliver to one peer id only, ignoring subscriptions. */
    to?: string | undefined
    /** Override the hub's retain rule for this payload. */
    retain?: boolean | undefined
  }

  export interface Options<T extends Protocol.Topics> {
    policy?: Policy<T> | undefined
    /** Narrow inbound payloads at the trust boundary before they are routed. */
    validate?: Protocol.Validators<T> | undefined
    /**
     * Channels whose last payload is replayed to each new subscriber, marked
     * `meta.retained`. Without this every consumer hand-rolls a
     * "send me the current state" channel.
     */
    retain?: readonly (keyof T & string)[] | ((channel: keyof T & string) => boolean) | undefined
    /** A throwing policy hook or watcher, or a rejected frame. */
    onError?: ((err: unknown) => void) | undefined
    /** Closes the hub when aborted, which closes every peer and stops every source. */
    signal?: AbortSignal | undefined
  }
}

/** Not public: one accepted peer's bookkeeping. */
type Entry<T extends Protocol.Topics> = {
  readonly id: string
  readonly link: Link<Protocol.Envelope>
  readonly channels: Set<keyof T & string>
  peer: Hub.Peer<T>
  /** False until `canAccept` has allowed it. Pending peers are invisible. */
  live: boolean
  /** Frames held while `canAccept` is outstanding. */
  held: Protocol.Frame[] | null
  stops: Detacher
  remove(): void
}

export class Hub<T extends Protocol.Topics = Protocol.Topics> {
  #peers = new Map<string, Entry<T>>()
  #snapshot: readonly Hub.Peer<T>[] | null = null
  #watchers: Emitter<[readonly Hub.Peer<T>[]]>
  #gone = emitter<[]>()
  #serving = new Set<Unsub>()
  #retained = new Map<string, unknown>()
  #retains: (channel: keyof T & string) => boolean
  #policy: Hub.Policy<T>
  #validate: Protocol.Validators<T>
  #closed = false
  #seq = 0

  static create = <T extends Protocol.Topics = Protocol.Topics>(name: string, opts: Hub.Options<T> = {}): Hub<T> =>
    new Hub<T>(name, opts)

  private constructor(
    public readonly name: string,
    private readonly opts: Hub.Options<T> = {},
  ) {
    this.#policy = opts.policy ?? {}
    this.#validate = opts.validate ?? {}
    const retain = opts.retain
    this.#retains =
      typeof retain === 'function' ? retain : retain ? (c) => (retain as readonly string[]).includes(c) : () => false
    this.#watchers = emitter<[readonly Hub.Peer<T>[]]>((err) => this.#fail(err))
    const signal = opts.signal
    if (signal?.aborted) this.close()
    else signal?.addEventListener('abort', () => this.close(), { once: true })
  }

  /**
   * Stable between changes, so it is safe to hand straight to
   * `useSyncExternalStore`. Both the array and each `Peer` keep identity until
   * that peer actually changes, at either granularity.
   *
   */
  get peers(): readonly Hub.Peer<T>[] {
    if (this.#snapshot) return this.#snapshot
    const out: Hub.Peer<T>[] = []
    for (const e of this.#peers.values()) if (e.live) out.push(e.peer)
    return (this.#snapshot = out)
  }

  peer(id: string): Hub.Peer<T> | null {
    const e = this.#peers.get(id)
    return e && e.live ? e.peer : null
  }

  onPeersChange(fn: (peers: readonly Hub.Peer<T>[]) => void, opts: ListenOptions = {}): Unsub {
    if (this.#closed) {
      opts.onClose?.()
      return noop
    }
    return this.#withEnd(this.#watchers.add(fn, opts.signal), opts)
  }

  /**
   * Take peers: one link, a source of them, or several of either.
   *
   * The hub takes ownership of every link it is given — from here it may close
   * one, and closing it is how a rejected or kicked peer finds out — so a link
   * must be served to exactly one hub.
   *
   * Explicit, and returns a teardown that stops the sources and closes what
   * they produced. A hub that registered a global listener in its constructor
   * could never be disposed or tested.
   *
   * @example
   * ```ts
   * hub.serve(worker('./a.ts'), worker('./b.ts')) // lazy: spawned here
   * hub.serve(persistent(() => link(new Worker('./c.ts')))) // reconnecting
   * hub.serve(handshake(self)) // peers that transfer their own port
   * ```
   */
  serve<E extends Protocol.Envelope>(...sources: Link.Servable<E>[]): Unsub {
    if (this.#closed) {
      for (const s of sources) if (typeof s !== 'function') s.close()
      return noop
    }
    const offs = sources.map((s) => (typeof s === 'function' ? s((link) => void this.#accept(link)) : this.#accept(s)))
    let done = false
    const stop = () => {
      if (done) return
      done = true
      this.#serving.delete(stop)
      for (const off of offs) off()
    }
    this.#serving.add(stop)
    return stop
  }

  /** Register one link as a peer. Returns a teardown that deregisters and closes it. */
  #accept(link: Link<Protocol.Envelope>): Unsub {
    if (this.#closed) {
      link.close()
      return () => {}
    }

    const id = `${this.name}#${++this.#seq}`
    const channels = new Set<keyof T & string>()

    const entry: Entry<T> = {
      id,
      link,
      channels,
      live: false,
      held: [],
      stops: detacher(),
      peer: {
        id,
        name: link.remote,
        meta: link.meta,
        channels: new Set<keyof T & string>(),
        rev: 0,
        send: (c, d) => link.send(Protocol.seal(this.name, { t: 'data', c, d })),
        close: () => link.close(),
      },
      remove: () => {
        entry.stops.stop()
        const was = entry.live
        if (this.#peers.delete(id) && was) this.#changed()
      },
    }

    this.#peers.set(id, entry)
    // `closed` fires synchronously here if the link is already dead, so `remove`
    // may run mid-attach. The detacher runs each teardown as it arrives once it
    // has stopped, instead of holding one that nobody will ever call.
    entry.stops.add(link.listen((m) => this.#recv(id, m)))
    entry.stops.add(link.closed(entry.remove))
    if (!this.#peers.has(id)) return () => {}

    const verdict = attempt<boolean | Promise<boolean>>(
      () => this.#policy.canAccept?.(entry.peer) ?? true,
      false,
      (err) => this.#fail(err),
    )

    if (isPromise(verdict)) {
      verdict.then(
        (ok) => (ok === false ? this.#reject(entry) : this.#admit(entry)),
        (err) => {
          this.#fail(err)
          this.#reject(entry)
        },
      )
    } else if (verdict === false) {
      this.#reject(entry)
    } else {
      this.#admit(entry)
    }

    return () => {
      entry.remove()
      link.close()
    }
  }

  /** A session backed by an in-memory pair — the hub's own process as a peer. */
  local(opts?: Session.Options<T>): Session<T> {
    const [mine, theirs] = pair<Protocol.Envelope>(`${this.name}:local`, this.name)
    this.#accept(theirs)
    return Session.over(this.name, mine, { ...opts, validate: opts?.validate ?? this.#validate })
  }

  publish<K extends keyof T & string>(channel: K, payload: T[K], opts: Hub.PublishOptions = {}): void {
    if (opts.retain ?? this.#retains(channel)) this.#retained.set(channel, payload)
    if (opts.to === undefined) {
      this.#fanout(null, channel, payload)
      return
    }
    const target = this.#peers.get(opts.to)
    if (target?.live) target.link.send(Protocol.seal(this.name, { t: 'data', c: channel, d: payload }))
  }

  /** Forget a retained value. New subscribers get nothing until the next publish. */
  clearRetained(channel?: keyof T & string): void {
    if (channel === undefined) this.#retained.clear()
    else this.#retained.delete(channel)
  }

  close(): void {
    if (this.#closed) return
    this.#closed = true
    for (const stop of [...this.#serving]) stop()
    for (const e of [...this.#peers.values()]) {
      e.remove()
      e.link.close()
    }
    this.#peers.clear()
    this.#retained.clear()
    this.#snapshot = null
    this.#gone.emit()
    this.#gone.clear()
    this.#watchers.clear()
  }

  #withEnd(off: Unsub, opts: ListenOptions): Unsub {
    const onClose = opts.onClose
    if (!onClose) return off
    const offGone = this.#gone.add(() => onClose(), opts.signal)
    return () => {
      off()
      offGone()
    }
  }

  #fail(err: unknown): void {
    report(err, this.opts.onError)
  }

  #reject(entry: Entry<T>): void {
    entry.remove()
    entry.link.close()
  }

  #admit(entry: Entry<T>): void {
    if (!this.#peers.has(entry.id)) return
    if (this.#closed) {
      this.#reject(entry)
      return
    }
    entry.live = true
    const held = entry.held ?? []
    entry.held = null
    this.#changed()

    // Unprompted greeting, carrying the id peers address each other by.
    // Everything above uses its arrival to tell a live-but-quiet connection
    // from one that never landed.
    entry.link.send(Protocol.seal(this.name, { t: 'ready', id: entry.id }))
    for (const f of held) this.#handle(entry, f)
  }

  #changed(): void {
    this.#snapshot = null
    this.#watchers.emit(this.peers)
  }

  /** Mint a fresh `Peer` so identity tracks subscription state. */
  #touch(entry: Entry<T>): void {
    entry.peer = { ...entry.peer, channels: new Set(entry.channels), rev: entry.peer.rev + 1 }
  }

  #recv(id: string, m: Protocol.Envelope): void {
    const entry = this.#peers.get(id)
    if (!entry) return
    const r = Protocol.unseal(this.name, m)
    if (!r.ok) {
      if (r.reason !== 'foreign') {
        this.#fail(
          new Protocol.ProtocolError(r.reason, `discarded a ${r.reason} frame from ${entry.peer.name || id}`, {
            hub: this.name,
            peer: id,
            version: r.version,
          }),
        )
      }
      return
    }
    if (entry.held) {
      entry.held.push(r.frame)
      return
    }
    this.#handle(entry, r.frame)
  }

  #handle(entry: Entry<T>, f: Protocol.Frame): void {
    switch (f.t) {
      case 'sub': {
        const c = f.c as keyof T & string
        if (
          attempt(
            () => this.#policy.canSubscribe?.(entry.peer, c) ?? true,
            false,
            (e) => this.#fail(e),
          ) === false
        ) {
          return
        }
        if (entry.channels.has(c)) return
        entry.channels.add(c)
        this.#touch(entry)
        this.#changed()
        if (this.#retained.has(c)) {
          entry.link.send(Protocol.seal(this.name, { t: 'data', c, d: this.#retained.get(c), r: true }))
        }
        return
      }
      case 'unsub': {
        if (!entry.channels.delete(f.c as keyof T & string)) return
        this.#touch(entry)
        this.#changed()
        return
      }
      case 'data': {
        const c = f.c as keyof T & string
        const check = this.#validate[c]
        let payload = f.d as T[typeof c]
        if (check) {
          const ok = validateSync(check, f.d)
          if (!ok.ok) {
            this.#fail(
              new Protocol.ProtocolError(
                'payload',
                `rejected a payload on "${c}" from ${entry.peer.name || entry.id}`,
                {
                  hub: this.name,
                  channel: c,
                  peer: entry.id,
                },
              ),
            )
            return
          }
          payload = ok.value as T[typeof c]
        }
        if (
          attempt(
            () => this.#policy.canPublish?.(entry.peer, c, payload) ?? true,
            false,
            (e) => this.#fail(e),
          ) === false
        ) {
          return
        }
        if (f.to !== undefined) {
          this.#direct(entry, f.to, c, payload)
          return
        }
        if (this.#retains(c)) this.#retained.set(c, payload)
        this.#fanout(entry.id, c, payload)
        return
      }
      // Peers do not greet.
      default:
        return
    }
  }

  #direct(from: Entry<T>, to: string, c: keyof T & string, d: unknown): void {
    const target = this.#peers.get(to)
    if (!target?.live) return
    if (
      attempt(
        () => this.#policy.canAddress?.(from.peer, target.peer, c) ?? true,
        false,
        (e) => this.#fail(e),
      ) === false
    ) {
      return
    }
    target.link.send(Protocol.seal(this.name, { t: 'data', c, d, from: from.id }))
  }

  #fanout(from: string | null, c: keyof T & string, d: unknown): void {
    // One envelope for every recipient: `from` is a fact about the sender, not
    // the receiver, so it does not vary per peer.
    const env =
      from === null
        ? Protocol.seal(this.name, { t: 'data', c, d })
        : Protocol.seal(this.name, { t: 'data', c, d, from })
    for (const [id, e] of this.#peers) {
      if (!e.live || id === from || !e.channels.has(c)) continue
      // No dead-peer sweep: `send` is inert once closed, and `closed` already
      // removes the entry.
      e.link.send(env)
    }
  }
}
