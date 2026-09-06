import { emitter, noop, repeat, type Emitter } from '../core/internal.ts'
import type { Port, Unsub } from '../index.ts'
import { systemClock, type Clock } from '../core/index.ts'

/**
 * A live view of replicated state.
 *
 * A replica connects to its port — and, for followers, begins joining — on
 * construction, not on subscription, so {@link Replica.get} can never quietly
 * answer from a view that stopped tracking. It can still answer `initial` from
 * a view that has not joined yet; {@link Replica.status} distinguishes that
 * from a genuinely initial state.
 *
 * Call {@link Replica.close} to release it; this ends the view for subscribers
 * and announces the departure so peers can forget the id.
 */
export interface Replica<S, T extends Replica.Type> {
  readonly type: T
  /** Scopes this replica's traffic on the port. See {@link Replica.Def.name}. */
  readonly name: string | undefined
  /** This participant's id on the port. */
  readonly id: string
  readonly status: Replica.Status
  /**
   * Set when the replica closed on a failure: a reducer threw on remote input,
   * or the port closed with an error. Undefined after a clean close — or, as a
   * consequence, after a reducer that threw `undefined`.
   */
  readonly error: unknown
  get(): S
  /**
   * Fires after each state change. Nothing is replayed on subscribe; read
   * `get()` for the current value. `onClose` fires when the replica closes,
   * with the error if it failed.
   */
  listen(fn: (state: S) => void, opts?: Port.ListenOptions): Unsub
  /** Fires on each status transition. */
  onStatus(fn: (status: Replica.Status) => void, opts?: Port.ListenOptions): Unsub
  close(): void
}

export declare namespace Replica {
  export type Type = 'R' | 'W' | 'RW'

  /**
   * - `joining` — waiting on a snapshot; the view still holds `initial`.
   * - `live` — tracking replicated state.
   * - `closed` — released; `error` says whether by failure.
   */
  export type Status = 'joining' | 'live' | 'closed'

  /** A replica that can only observe state. */
  export interface Reader<S> extends Replica<S, 'R'> {}

  /** The authoritative replica: it writes locally and never adopts remote state. */
  export interface Writer<S, U> extends Replica<S, 'W'> {
    update: (u: U) => void
  }

  /** A peer replica: it follows remote updates and produces its own. */
  export interface ReaderWriter<S, U> extends Replica<S, 'RW'> {
    update: (u: U) => void
  }

  /**
   * The wire protocol shared by every replica.
   *
   * `version` is a *per-sender* sequence number, not a version of the state. Two
   * participants may both be at `1` without conflict; what matters is that each
   * sender's own sequence arrives contiguously.
   *
   * `name` scopes a message to its definition, so several replicated states can
   * share one port; unnamed traffic only reaches unnamed replicas.
   *
   * - `req` asks for a snapshot, either from one peer (`to`) or from anyone.
   * - `snapshot` carries state plus the sender's full {@link Replica.Versions}
   *   view, so adopting it does not lose track of third parties.
   * - `update` broadcasts one step of the sender's sequence.
   * - `bye` announces a departure so peers can drop the id. Ids are per instance,
   *   so without it a reconnecting client leaves an entry that every subsequent
   *   snapshot propagates to everyone else.
   */
  export type Message<S, U> =
    | { type: 'req'; name?: string | undefined; from: string; to?: string | undefined }
    | {
        type: 'snapshot'
        name?: string | undefined
        from: string
        to?: string | undefined
        versions: Versions
        snapshot: S
      }
    | { type: 'update'; name?: string | undefined; from: string; version: number; update: U }
    | { type: 'bye'; name?: string | undefined; from: string }

  /** Last sequence number seen from each participant, keyed by id. */
  export type Versions = Record<string, number>

  export type Update<S, U> = Extract<Replica.Message<S, U>, { type: 'update' }>

  /**
   * How a follower joins the port.
   *
   * On construction it broadcasts a `req` and buffers incoming updates. Every
   * `every` ms without an answer it asks again, `attempts` times in total; one
   * period after the last unanswered ask it gives up, goes live from `initial`,
   * and replays the buffer. A snapshot or a local write ends the wait early.
   */
  export interface JoinPolicy {
    /** Milliseconds between snapshot requests. Default `300`. */
    every?: number | undefined
    /** Requests sent before giving up and going live. Default `3`. */
    attempts?: number | undefined
    /**
     * Updates held while joining. Default `1024`. Past it the oldest is dropped;
     * the hole that leaves is detected on replay and recovered as a gap.
     */
    limit?: number | undefined
  }

  /**
   * How a follower recovers updates it missed.
   *
   * A gap does not request anything immediately: the update is held and the first
   * `req` goes out `every` ms later, so a transport that merely reordered two
   * messages resolves without a round trip. Only one request per peer is in
   * flight at a time, so a chatty peer cannot amplify one gap into one request
   * per message. The final attempt is broadcast rather than directed, since a
   * third party may hold the updates of a peer that has since left.
   *
   * After `attempts` the chase stops and that peer's stream stays blocked; the
   * next update from it starts a fresh chase.
   */
  export interface GapPolicy {
    /** Milliseconds between requests, and the grace period for reordering. Default `300`. */
    every?: number | undefined
    /** Requests sent before giving up until the peer speaks again. Default `3`. */
    attempts?: number | undefined
    /**
     * Out-of-order updates held per peer. Default `256`. Past it the
     * furthest-ahead is dropped, keeping those nearest the gap.
     */
    limit?: number | undefined
  }

  export interface Def<S, U = S> {
    /**
     * Scopes messages on the port: outgoing traffic is stamped with the name
     * and anything carrying a different one is ignored, so several definitions
     * can share one transport. Unnamed replicas only see unnamed traffic.
     */
    name?: string | undefined
    /**
     * Identifies this participant on the port. Must be unique per instance:
     * two instances sharing an id mutually suppress each other as self-echo and
     * diverge silently. Omit it for a generated one.
     */
    id?: string | undefined
    initial: S
    /** Pure reducer folding an update into state. */
    apply: (state: S, update: U) => S
    /**
     * Reconciles local state with an adopted snapshot. Defaults to taking the
     * snapshot wholesale, which discards local updates the sender had not seen.
     * Required by {@link readerWriter}, where that default would drop the
     * replica's own unacknowledged writes.
     */
    merge?: ((mine: S, theirs: S) => S) | undefined
    /**
     * Snapshot request policy for followers; ignored by a {@link writer}.
     * `false` skips the join entirely: the replica is live from `initial` at
     * construction and still recovers gaps with directed requests.
     */
    join?: JoinPolicy | false | undefined
    /** Gap recovery policy for followers; ignored by a {@link writer}. */
    gap?: GapPolicy | undefined
    /**
     * Milliseconds a peer replica waits before answering a *broadcast* `req`,
     * cancelling if it sees another answer first. Without it every member of a
     * mesh answers each join, so one joiner costs N snapshots. Default `100`;
     * `0` answers at once. The delay is randomised within this bound, so tests
     * that want it deterministic pin `Math.random` or pass `0`.
     *
     * Directed requests and a {@link writer}'s answers are never delayed.
     */
    jitter?: number | undefined
    /** Drives the join, gap and offer timers. Pass `fakeClock()` from `@lickle/wire/testing` in tests. */
    clock?: Clock | undefined
  }

  /** A definition that can reconcile concurrent writes. See {@link Replica.Def.merge}. */
  export interface MergeDef<S, U = S> extends Def<S, U> {
    merge: (mine: S, theirs: S) => S
  }
}

type Factories<S, U> = {
  r: (ch: Port.Port<Replica.Message<S, U>>, id?: string) => Replica.Reader<S>
  w: (ch: Port.Port<Replica.Message<S, U>>, id?: string) => Replica.Writer<S, U>
}

type MergingFactories<S, U> = Factories<S, U> & {
  rw: (ch: Port.Port<Replica.Message<S, U>>, id?: string) => Replica.ReaderWriter<S, U>
}

/**
 * Binds a definition to the three constructors. `rw` is only offered when the
 * definition supplies a `merge`, since a peer replica without one loses its own
 * writes the first time it adopts a snapshot.
 *
 * Omit the id to have one generated; pass one only when it must survive a
 * reconnect, and never share it between two live instances.
 *
 * @example
 * ```ts
 * const counter = define({ initial: 0, apply: (n, d: number) => n + d })
 * const w = counter.w(session.topic('counter'))
 * const r = counter.r(other.topic('counter'), 'pinned-id')
 * ```
 */
export function define<S, U>(def: Omit<Replica.MergeDef<S, U>, 'id'>): MergingFactories<S, U>
export function define<S, U>(def: Omit<Replica.Def<S, U>, 'id'>): Factories<S, U>
export function define<S, U>(def: Omit<Replica.Def<S, U>, 'id'>): MergingFactories<S, U> {
  const with_ = (id: string | undefined): Replica.Def<S, U> => (id === undefined ? { ...def } : { ...def, id })
  return {
    r: (ch, id) => reader(with_(id), ch),
    w: (ch, id) => writer(with_(id), ch),
    rw: (ch, id) => readerWriter(with_(id) as Replica.MergeDef<S, U>, ch),
  }
}

/**
 * Creates a follower replica. It joins per {@link Replica.JoinPolicy}, applies
 * each peer's updates in sequence, and recovers from any it falls behind per
 * {@link Replica.GapPolicy}. It produces nothing of its own and never answers
 * requests.
 */
export const reader = <S, U>(def: Replica.Def<S, U>, ch: Port.Port<Replica.Message<S, U>>): Replica.Reader<S> =>
  sync(def, ch, { follow: true, serve: false }).view('R')

/**
 * Creates the authoritative replica. It applies updates locally, broadcasts
 * them, and answers `req` with a snapshot. It never accepts remote updates, so
 * it is never stale. Do not mix with other writing replicas on one port.
 */
export const writer = <S, U>(def: Replica.Def<S, U>, ch: Port.Port<Replica.Message<S, U>>): Replica.Writer<S, U> => {
  const { view, update } = sync(def, ch, { follow: false, serve: true })
  // Assigned onto, not spread: spreading would read the live getters once.
  return Object.assign(view('W'), { update })
}

/**
 * Creates a peer replica: it follows remote updates like a {@link reader},
 * produces its own like a {@link writer}, and answers `req` once live — which
 * is what lets a late joiner catch up in a mesh with no authoritative writer.
 *
 * Sequence numbers are per-sender, so concurrent writes are not a conflict at
 * the protocol level — both are delivered, in whatever order the transport
 * chooses. Convergence is therefore the reducer's job: `apply` should be
 * commutative across participants, and `merge` — which this constructor
 * requires — reconciles the states that result when it isn't.
 */
export const readerWriter = <S, U>(
  def: Replica.MergeDef<S, U>,
  ch: Port.Port<Replica.Message<S, U>>,
): Replica.ReaderWriter<S, U> => {
  const { view, update } = sync(def, ch, { follow: true, serve: true })
  return Object.assign(view('RW'), { update })
}

/** What we know about one other participant. */
type Peer<U> = {
  /** Last sequence number applied from them. */
  version: number
  /** Updates past the gap, held until it closes. Keyed by sequence number. */
  pending: Map<number, U>
  /** Live gap chase, if any. */
  chase?: Unsub | undefined
}

const rid = (): string => {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto
  return c?.randomUUID?.() ?? `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`
}

/**
 * The shared synchronisation core.
 *
 * @param opts.follow - Whether remote updates and snapshots are accepted. A
 *   {@link writer} sets this to `false`: it is the sole authority.
 * @param opts.serve - Whether to answer `req` once live. A snapshot travels
 *   with `versions`, so even a view that is itself behind is safe to serve:
 *   the requester adopts that position too, and chases whatever it is missing.
 */
const sync = <S, U>(
  def: Replica.Def<S, U>,
  ch: Port.Port<Replica.Message<S, U>>,
  opts: { follow: boolean; serve: boolean },
) => {
  const id = def.id ?? rid()
  /** Our own sequence number. */
  let version = 0
  /** What we know about each other participant. */
  const peers = new Map<string, Peer<U>>()
  /** Peers we have asked for a snapshot and are waiting on. */
  const awaiting = new Set<string>()
  /** Broadcast requests we have scheduled an answer to, by requester. */
  const offers = new Map<string, Unsub>()
  /** Updates that arrived while joining, replayed once live. */
  let buffer: Replica.Update<S, U>[] = []
  let stopJoin: Unsub | undefined
  let off: Unsub | undefined
  let closed = false

  const joinPolicy = def.join === false ? undefined : def.join
  const clock = def.clock ?? systemClock

  let state: S = def.initial
  let status: Replica.Status = opts.follow ? 'joining' : 'live'
  let error: unknown

  const stateChanged = emitter<[S]>()
  const statusChanged = emitter<[Replica.Status]>()
  /** Terminal. Emits the failure, or undefined on a clean close. */
  const gone = emitter<[unknown]>()

  const live = () => status === 'live'
  const setState = (next: S) => {
    state = next
    stateChanged.emit(next)
  }
  const setStatus = (next: Replica.Status) => {
    if (next === status) return
    status = next
    statusChanged.emit(next)
  }

  const send = (msg: Replica.Message<S, U>) => {
    if (!closed) ch.send(msg)
  }
  const req = (to?: string) => send({ type: 'req', name: def.name, from: id, to })

  const peer = (from: string): Peer<U> => {
    let p = peers.get(from)
    if (!p) peers.set(from, (p = { version: 0, pending: new Map() }))
    return p
  }

  const versions = (): Replica.Versions => {
    // Null-prototyped: ids come off the wire, and a `__proto__` key survives
    // `JSON.parse` as an own property that a plain object would honour.
    const out: Replica.Versions = Object.create(null)
    for (const [pid, p] of peers) out[pid] = p.version
    out[id] = version
    return out
  }

  const offer = (to?: string) =>
    send({ type: 'snapshot', name: def.name, from: id, to, versions: versions(), snapshot: state })

  const teardown = () => {
    stopJoin?.()
    stopJoin = undefined
    for (const p of peers.values()) p.chase?.()
    for (const stop of offers.values()) stop()
    offers.clear()
    peers.clear()
    awaiting.clear()
    buffer = []
    off?.()
  }

  /** The one terminal path. `err` undefined is a clean close. */
  const end = (err?: unknown) => {
    if (closed) return
    closed = true
    error = err
    teardown()
    setStatus('closed')
    gone.emit(err)
    stateChanged.clear()
    statusChanged.clear()
    gone.clear()
  }

  /** A reducer threw on remote input: there is no consistent state to carry on from. */
  const fail = (e: unknown) => end(e)

  /** Stop chasing a peer, whether the gap closed or the peer left. */
  const unchase = (from: string) => {
    const p = peers.get(from)
    p?.chase?.()
    if (p) p.chase = undefined
    awaiting.delete(from)
  }

  /**
   * Start chasing a peer's missing updates, if we are not already. The first
   * request goes out one period from now, so a reorder resolves for free.
   */
  const chase = (from: string) => {
    const p = peer(from)
    if (p.chase || closed) return
    awaiting.add(from)
    const { every = 300, attempts = 3 } = def.gap ?? {}
    p.chase = repeat(clock, { delay: every, period: every, count: attempts + 1 }, (n) => {
      // The last ask goes to the whole port: this peer may be unreachable, but
      // anyone who saw its updates can answer with a snapshot.
      if (n < attempts) return req(n < attempts - 1 ? from : undefined)
      // Exhausted. Release the handle so a later update starts a fresh chase.
      p.chase = undefined
    })
  }

  /** Hold an update we cannot apply yet, evicting the furthest-ahead if full. */
  const hold = (p: Peer<U>, v: number, u: U) => {
    const { limit = 256 } = def.gap ?? {}
    if (p.pending.size >= limit) {
      let far = -1
      for (const held of p.pending.keys()) if (held > far) far = held
      if (v > far) return // the newcomer is the least useful; keep those nearest the gap
      p.pending.delete(far)
    }
    p.pending.set(v, u)
  }

  /**
   * Apply everything contiguous a peer has held. Returns false if the reducer
   * threw or the replica has since closed.
   */
  const drain = (from: string): boolean => {
    if (closed) return false
    const p = peer(from)
    let next = state
    let advanced = false
    try {
      for (;;) {
        const u = p.pending.get(p.version + 1)
        if (u === undefined) break
        // Fold first, then record: a throw must not leave us claiming an
        // update we did not apply.
        next = def.apply(next, u)
        p.pending.delete(++p.version)
        advanced = true
      }
    } catch (e) {
      fail(e)
      return false
    }
    if (advanced) setState(next)
    return !closed
  }

  const receive = (msg: Replica.Update<S, U>) => {
    const p = peer(msg.from)
    if (msg.version <= p.version || p.pending.has(msg.version)) return // duplicate or replay
    hold(p, msg.version, msg.update)
    if (msg.version > p.version + 1) return chase(msg.from) // out of order, or lost
    if (!drain(msg.from)) return
    if (p.pending.size === 0) unchase(msg.from)
  }

  const goLive = () => {
    if (live() || closed) return
    stopJoin?.()
    stopJoin = undefined
    setStatus('live')
    const pending = buffer
    buffer = []
    // Replay through the normal path: whatever an adopted snapshot already
    // covered drops as a replay; anything past it applies or opens a gap.
    for (const msg of pending) {
      if (closed) return
      receive(msg)
    }
  }

  const adopt = (msg: { from: string; versions: Replica.Versions; snapshot: S }) => {
    let next: S
    try {
      next = def.merge ? def.merge(state, msg.snapshot) : msg.snapshot
    } catch (e) {
      return fail(e)
    }

    /** Peers whose position moved, and where it stood before. */
    const moved = new Map<string, number>()
    // A snapshot's state and its versions travel together, so adopting it means
    // adopting its position on every third party — including where that is
    // *behind* ours. Taking the max here would leave us claiming updates the
    // state we just took does not contain, and nothing would ever notice: the
    // third party's next update would arrive contiguous with a version we no
    // longer have the effects of.
    for (const pid of Object.keys(msg.versions)) {
      if (pid === id) continue
      const theirs = msg.versions[pid] as number
      const p = peer(pid)
      const was = p.version
      p.version = theirs
      for (const held of [...p.pending.keys()]) if (held <= theirs) p.pending.delete(held)
      // The snapshot covers this peer through `theirs`, so an outstanding chase
      // is satisfied. Any that is still needed is restarted below, with a fresh
      // set of attempts.
      unchase(pid)
      if (was !== theirs || p.pending.size > 0) moved.set(pid, was)
    }

    setState(next)
    if (closed) return
    for (const [pid, was] of moved) {
      if (!drain(pid)) return
      const p = peer(pid)
      // Either updates are still held past a gap the snapshot did not close, or
      // we went backwards and could not replay our way forward again.
      if (p.pending.size > 0 || p.version < was) chase(pid)
    }
    goLive()
  }

  const join = () => {
    if (def.join === false) return goLive()
    const { every = 300, attempts = 3 } = joinPolicy ?? {}
    stopJoin = repeat(clock, { delay: 0, period: every, count: attempts + 1 }, (n) => (n < attempts ? req() : goLive()))
  }

  /**
   * Answer a broadcast request after a short random delay, standing down if
   * someone else answers first. One joiner should not cost N snapshots.
   */
  const schedule = (to: string) => {
    if (offers.has(to)) return
    const jitter = def.jitter ?? 100
    if (jitter <= 0) return offer(to)
    const delay = Math.max(1, Math.round(Math.random() * jitter))
    let fired = false
    const stop = repeat(clock, { delay, count: 1 }, () => {
      fired = true
      offers.delete(to)
      offer(to)
    })
    if (!fired) offers.set(to, stop)
  }

  const standDown = (to: string) => {
    offers.get(to)?.()
    offers.delete(to)
  }

  off = ch.listen(
    (msg) => {
      if (closed) return
      if (msg.name !== def.name) return // another definition's traffic
      if (msg.from === id) return // self-echo

      if (msg.type === 'bye') {
        standDown(msg.from)
        if (!opts.follow) return
        unchase(msg.from)
        peers.delete(msg.from)
        return
      }

      if (msg.type === 'req') {
        if (msg.to !== undefined && msg.to !== id) return
        if (!opts.serve || !live()) return
        // Directed, or we are the sole authority: no reason to wait.
        if (msg.to === id || !opts.follow) return offer(msg.from)
        return schedule(msg.from)
      }

      if (!opts.follow) return // a writer is the sole authority

      if (msg.type === 'snapshot') {
        // Someone beat us to answering this requester.
        if (msg.to !== undefined) standDown(msg.to)
        // Accept one we asked for — including an answer overheard on its way
        // to someone else, since a chased peer's state helps regardless of the
        // addressee — or anything at all while still joining.
        if (!awaiting.delete(msg.from) && live()) return
        return adopt(msg)
      }

      if (!live()) {
        const { limit = 1024 } = joinPolicy ?? {}
        // Oldest first: the hole reads as a gap on replay and is recovered.
        if (buffer.length >= limit) buffer.shift()
        buffer.push(msg)
        return
      }
      receive(msg)
    },
    { onClose: end },
  )
  // The port may have delivered, or closed, synchronously and already torn us down.
  if (closed) off()

  const update = (u: U) => {
    if (closed) return
    // A local write ends the join: buffered remote ops land first, then ours.
    goLive()
    // Fold before mutating anything, so a throwing reducer propagates to the
    // caller without burning a sequence number that peers would then wait for.
    const next = def.apply(state, u)
    version++
    setState(next)
    send({ type: 'update', name: def.name, from: id, version, update: u })
  }

  const close = () => {
    if (closed) return
    // Only participants that can appear in someone's `peers` need announcing.
    if (opts.serve || version > 0) send({ type: 'bye', name: def.name, from: id })
    end()
  }

  /** Shared by `listen` and `onStatus`: a subscription whose `onClose` is the replica's end. */
  const on = <A extends unknown[]>(em: Emitter<A>, fn: (...a: A) => void, o: Port.ListenOptions = {}): Unsub => {
    if (closed) {
      o.onClose?.(error)
      return noop
    }
    if (o.signal?.aborted) return noop
    const offFn = em.add(fn, o.signal)
    const onClose = o.onClose
    if (!onClose) return offFn
    const offGone = gone.add((e) => onClose(e), o.signal)
    return () => {
      offFn()
      offGone()
    }
  }

  const view = <T extends Replica.Type>(type: T): Replica<S, T> => ({
    type,
    name: def.name,
    id,
    get status() {
      return status
    },
    get error() {
      return error
    },
    get: () => state,
    listen: (fn, o) => on(stateChanged, fn, o),
    onStatus: (fn, o) => on(statusChanged, fn, o),
    close,
  })

  if (opts.follow) join()

  return { view, update }
}
