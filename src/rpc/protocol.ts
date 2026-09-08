import { noop, queue, report } from '../core/internal.ts'
import type { ListenOptions, Port, Unsub } from '../core/index.ts'
import { validateSync, type Validate } from '../core/validate.ts'
import type {
  Api,
  Call,
  CheckDirs,
  CheckKeys,
  Desc,
  Dir,
  HandlerContext,
  Handlers,
  Meta,
  Mode,
  On,
  Re,
  RecvMsg,
  Reserved,
  SendMsg,
  Source,
  Spec,
  Subscription,
  CallOptions,
  Context,
} from './spec.js'

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/**
 * The framed representation. `_t` namespaces the protocol so unrelated traffic
 * on a shared transport is dropped rather than misread; `re` distinguishes a
 * response from a request.
 *
 * `meta` is the one open field: anything the two ends agree on rides there,
 * untyped and unvalidated, and the key is absent when nothing was attached.
 */
export interface Frame {
  _t: string
  kind: string
  id?: string | undefined
  re?: Re | undefined
  payload?: unknown
  meta?: Meta | undefined
}

/**
 * How a message is framed onto the wire and read back off it.
 *
 * `decode` is written in continuation style so a codec can drop frames it does
 * not recognise — that is what lets several protocols share one transport. The
 * same continuation is how a layer contributes what it alone knows: whatever it
 * passes `next` as a second argument joins the frame's own `meta`, and reaches
 * every listener and handler as if the sender had put it there.
 *
 * Nothing is needed in the other direction — `encode` is handed the whole
 * message, so a layer that wants to lift `meta` into its own envelope reads it
 * off `msg.meta`.
 */
export interface Codec<Wire, Inner = any> {
  encode: (msg: Inner) => Wire
  decode: (frame: Wire, next: (msg: Inner, meta?: Meta) => void) => void
}

/**
 * Outer layers win. They have already been unwrapped and vouched for by the
 * time an inner one runs, while the innermost thing of all is what the far side
 * said about itself.
 */
const merge = (below: Meta | undefined, above: Meta | undefined): Meta | undefined =>
  below === undefined ? above : above === undefined ? below : { ...below, ...above }

const compose = (outer: Codec<any>, inner: Codec<any>): Codec<any> => ({
  encode: (msg) => outer.encode(inner.encode(msg)),
  decode: (frame, next) =>
    outer.decode(frame, (mid, above) => inner.decode(mid, (msg, below) => next(msg, merge(below, above)))),
})

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/** What to do with a frame that names this protocol but violates it. */
export type OnInvalid = 'throw' | 'drop' | ((reason: string, frame: unknown) => void)

export interface Options {
  /** Default `'throw'`. Use `'drop'` or the callback when the far side is untrusted. */
  onInvalid?: OnInvalid | undefined
  /** A throwing `on.X` listener. Without it the error is rethrown from a fresh task. */
  onError?: ((err: unknown) => void) | undefined
}

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

/**
 * One side of a protocol: a {@link Port} at the message level, plus the
 * generated call surface, per-message inbound listeners, and `serve`.
 */
export type Sided<T extends Spec, D extends Dir> = Port<SendMsg<T, D>, RecvMsg<T, D>> &
  Api<T, D> & {
    /** Per-message inbound listeners, carrying the means to respond. */
    on: On<T, D>
    /** Wire up handlers for inbound messages. Returns a teardown. */
    serve: (handlers: Handlers<T, D>) => Unsub
  }

/**
 * A {@link symmetric} instance. Every message may travel either way, so both ends
 * of a channel get the same surface — there is nothing to pick between.
 */
export type Peer<T extends Spec> = Sided<T, 'l'>

export interface Protocol<T extends Spec, Wire = Frame> {
  readonly name: string
  readonly spec: T
  left: (transport: Port<Wire, Wire>, codec?: Codec<any>) => Sided<T, 'l'>
  right: (transport: Port<Wire, Wire>, codec?: Codec<any>) => Sided<T, 'r'>
  /** Re-frame through an outer codec. Composes; it does not replace. */
  with: <Outer>(codec: Codec<Outer, Wire>) => Protocol<T, Outer>
  /** Rename, so the same protocol can be mounted twice on one transport. */
  at: (name: string) => Protocol<T, Wire>
}

const RESERVED: ReadonlySet<string> = new Set<Reserved>(['send', 'listen', 'on', 'serve', 'then'])

/**
 * Names a bidirectional protocol.
 *
 * @example
 * ```ts
 * const api = pair('api', {
 *   log:     send<string>(),
 *   push:    recv<ServerEvent>(),
 *   ready:   both<void>(),
 *   getUser: send<{ id: string }>().reply<User>(),
 *   watch:   send<Query>().stream<Row>(),
 * })
 * ```
 */
export const pair = <const T extends Spec>(
  name: string,
  spec: T & CheckKeys<T>,
  options: Options = {},
): Protocol<T> => {
  guard(name, spec as T)
  return build(name, spec as T, options, null)
}

/**
 * A protocol with one kind of end rather than two: the spec is symmetric, so
 * every instance sends, receives and serves the same messages.
 */
export interface Channel<T extends Spec, Wire = Frame> {
  readonly name: string
  readonly spec: T
  /** Attach an instance. There is only one constructor because both ends are alike. */
  connect: (transport: Port<Wire, Wire>) => Peer<T>
  /** Re-frame through an outer codec. Composes; it does not replace. */
  with: <Outer>(codec: Codec<Outer, Wire>) => Channel<T, Outer>
  /** Rename, so the same channel can be mounted twice on one transport. */
  at: (name: string) => Channel<T, Wire>
}

/**
 * Names a symmetric protocol: one spec, one shape of end.
 *
 * Every message must be declared with `both()`, since there is no far side to
 * be different from. Otherwise it is a {@link pair} — same wire format, same
 * call surface, same `on` and `serve`.
 *
 * @example
 * ```ts
 * const room = symmetric('room', {
 *   chat:    both<string>(),
 *   ping:    both<void>().reply<number>(),
 *   history: both<Query>().stream<Row>(),
 * })
 *
 * const a = room.connect(portA)
 * const b = room.connect(portB)
 * b.serve({ ping: () => Date.now() })
 * a.chat('hi')
 * ```
 */
export const symmetric = <const T extends Spec>(
  name: string,
  spec: T & CheckKeys<T> & CheckDirs<T>,
  options: Options = {},
): Channel<T> => {
  guard(name, spec as T)
  for (const [k, d] of Object.entries(spec as T) as [string, Desc][]) {
    if (d.dir !== 'b') throw new Error(`[${name}] "${k}" is one-directional; a channel declares messages with both()`)
  }
  return asChannel(build<T, Frame>(name, spec as T, options, null))
}

const guard = (name: string, spec: Spec) => {
  for (const k of Object.keys(spec)) {
    if (RESERVED.has(k)) throw new Error(`[${name}] "${k}" is a reserved message name`)
  }
}

/** A channel is a protocol whose two sides coincide, so one of them is the instance. */
const asChannel = <T extends Spec, W>(p: Protocol<T, W>): Channel<T, W> => ({
  name: p.name,
  spec: p.spec,
  connect: p.left,
  with: (codec) => asChannel(p.with(codec)),
  at: (renamed) => asChannel(p.at(renamed)),
})

const build = <T extends Spec, W>(
  name: string,
  spec: T,
  options: Options,
  outer: Codec<any> | null,
): Protocol<T, W> => {
  const codecFor = (dir: Dir, cod?: Codec<any>): Codec<any> => {
    const inner = specCodec(name, spec, dir, options)
    const c1 = outer ? compose(outer, inner) : inner
    return cod ? compose(cod, c1) : c1
  }

  return {
    name,
    spec,
    left: (transport, c) => adapt(spec, 'l', codecFor('l', c), transport as unknown as Port<Frame, Frame>, options),
    right: (transport, c) => adapt(spec, 'r', codecFor('r', c), transport as unknown as Port<Frame, Frame>, options),
    with: <Outer>(codec: Codec<Outer, W>) =>
      build<T, Outer>(name, spec, options, outer ? compose(codec, outer) : codec),
    at: (renamed) => build<T, W>(renamed, spec, options, outer),
  }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

const flip = (d: Dir): Dir => (d === 'l' ? 'r' : 'l')
const allows = (declared: Dir, side: Dir) => declared === 'b' || declared === side

const specCodec = (name: string, spec: Spec, side: Dir, options: Options): Codec<Frame> => {
  const reject = (reason: string, frame: unknown) => {
    const onInvalid = options.onInvalid ?? 'throw'
    if (onInvalid === 'drop') return
    if (typeof onInvalid === 'function') return onInvalid(reason, frame)
    throw new Error(`[${name}] ${reason}`)
  }

  const validate = (schema: Validate | undefined, payload: unknown, label: string, frame: Frame) => {
    if (!schema) return true
    const result = validateSync(schema, payload)
    if (!result.ok) {
      reject(`${label}: ${result.message}`, frame)
      return false
    }
    return true
  }

  return {
    encode: (msg) => ({ _t: name, ...msg }),
    decode: (frame, next) => {
      if (!frame || typeof frame !== 'object' || frame._t !== name) return

      const { _t, ...msg } = frame
      const d: Desc | undefined = spec[frame.kind]
      if (!d) return reject(`unknown message: ${String(frame.kind)}`, frame)
      // Meta is framing rather than payload, so its shape is checked here even
      // though its contents never are.
      if (frame.meta !== undefined && !isMeta(frame.meta)) return reject(`${frame.kind}: meta must be an object`, frame)

      // A request or a cancellation travels from originator to responder, so
      // the far side must be allowed to originate this message.
      if (frame.re === undefined || frame.re === 'stop') {
        if (!allows(d.dir, flip(side))) return reject(`peer may not send ${frame.kind}`, frame)
        if (d.mode !== 'none' && typeof frame.id !== 'string')
          return reject(`${frame.kind} is missing a correlation id`, frame)
        if (frame.re === undefined && !validate(d.req, frame.payload, `bad ${frame.kind} payload`, frame)) return
        return next(msg)
      }

      // A response travels the other way: we must have been the requester.
      if (!allows(d.dir, side)) return reject(`unexpected response for ${frame.kind}`, frame)
      if (d.mode === 'none') return reject(`${frame.kind} does not take a response`, frame)
      if (typeof frame.id !== 'string') return reject(`response to ${frame.kind} is missing a correlation id`, frame)
      if (
        (frame.re === 'ok' || frame.re === 'next') &&
        !validate(d.res, frame.payload, `bad ${frame.kind} response`, frame)
      )
        return
      next(msg)
    },
  }
}

// ---------------------------------------------------------------------------
// Errors across the wire
// ---------------------------------------------------------------------------

interface WireError {
  name: string
  message: string
}

const isWireError = (v: unknown): v is WireError =>
  !!v &&
  typeof v === 'object' &&
  typeof (v as WireError).message === 'string' &&
  typeof (v as WireError).name === 'string'

/** Errors have to survive structured clone, so send the shape rather than the instance. */
const toWire = (e: unknown) => (e instanceof Error ? { name: e.name, message: e.message } : e)
const fromWire = (e: unknown) => {
  if (!isWireError(e)) return e
  const err = new Error(e.message)
  err.name = e.name
  return err
}

const closedError = (kind: string, error: unknown) =>
  error === undefined ? new Error(`${kind}: channel closed before a response arrived`) : error

// ---------------------------------------------------------------------------
// Side adapter
// ---------------------------------------------------------------------------

type AnyMsg = {
  kind: string
  id?: string | undefined
  re?: Re | undefined
  payload?: unknown
  meta?: Meta | undefined
}
type Pending = { kind: string; handle: (msg: AnyMsg) => void; close: (error?: unknown) => void }
type Listener = { sink: (value: unknown, meta: Meta) => void; onClose: ((error?: unknown) => void) | undefined }
type Handler = (payload: unknown, ctx: HandlerContext) => unknown

const isMeta = (v: unknown): v is Meta => typeof v === 'object' && v !== null && !Array.isArray(v)

/** Frames carry `meta` only when there is some, so a protocol that never sets it is unchanged. */
const stamp = (msg: AnyMsg, meta: Meta | undefined): AnyMsg => (meta === undefined ? msg : { ...msg, meta })

/** What a codec contributed lands on the message itself, so a raw listener sees what `on.X` sees. */
const enrich = (msg: AnyMsg, meta: Meta | undefined): AnyMsg =>
  meta === undefined ? msg : { ...msg, meta: { ...msg.meta, ...meta } }

const SEP = String.fromCharCode(0)
const key = (kind: string, id: string) => `${kind}${SEP}${id}`

/**
 * `Args<Q>` is erased, so a lone argument is ambiguous between a payload and
 * `CallOptions`. The one thing a payload can never carry across a wire is a
 * live `AbortSignal`, so that is the first discriminator. Meta-only options
 * carry no signal, so they are recognised structurally instead: an object whose
 * every key is an option name, with a `meta` on it. A payload that genuinely
 * looks like that is sent by passing the options explicitly — `f(payload, {})`.
 */
const isOpts = (v: unknown): v is CallOptions => {
  if (typeof v !== 'object' || v === null) return false
  const o = v as CallOptions
  if (typeof AbortSignal !== 'undefined' && o.signal instanceof AbortSignal) return true
  return o.signal === undefined && isMeta(o.meta) && Object.keys(o).every((k) => k === 'meta' || k === 'signal')
}

const split = (args: unknown[]): [payload: unknown, opts: CallOptions | undefined] =>
  args.length >= 2
    ? [args[0], args[1] as CallOptions | undefined]
    : isOpts(args[0])
      ? [undefined, args[0]]
      : [args[0], undefined]

const adapt = <T extends Spec, D extends Dir>(
  spec: T,
  side: D,
  codec: Codec<any>,
  transport: Port<Frame, Frame>,
  options: Options,
): Sided<T, D> => {
  const send = (msg: AnyMsg) => transport.send(codec.encode(msg))

  /** Our outstanding calls and streams, by our correlation id. */
  const pending = new Map<string, Pending>()
  /** Inbound requests we may still be answering, by kind + their id: how a `stop` finds its handler. */
  const stops = new Map<string, (reason?: unknown) => void>()
  const listeners = new Map<string, Set<Listener>>()
  const raw = new Set<Listener>()
  let seq = 0

  // -- exactly one transport listener ----------------------------------------
  //
  // Held only while someone needs inbound frames: a pending request, an `on.X`
  // listener, or an inbound request that must hear its `stop`. A side that only
  // ever sends notifications holds nothing.

  let users = 0
  let off: Unsub | null = null
  let attaching = false
  let closed: { error: unknown } | null = null

  const attach = () => {
    attaching = true
    // A link flushes buffered inbound — and reports an already-closed port —
    // synchronously inside `listen`, so nothing may assume `off` is set until
    // this returns.
    const u = transport.listen((frame) => codec.decode(frame, (m, meta) => dispatch(enrich(m as AnyMsg, meta))), {
      onClose: shutdown,
    })
    attaching = false
    if (users === 0 || closed) u()
    else off = u
  }

  const acquire = (): Unsub => {
    users++
    if (users === 1 && !off && !attaching && !closed) attach()
    let released = false
    return () => {
      if (released) return
      released = true
      users--
      if (users === 0 && off) {
        const u = off
        off = null
        u()
      }
    }
  }

  // -- inbound ---------------------------------------------------------------

  const fire = (l: Listener, value: unknown, meta: Meta) => {
    try {
      l.sink(value, meta)
    } catch (err) {
      // One bad listener never starves the rest.
      report(err, options.onError)
    }
  }

  // Runs inside the transport's own delivery callback, so a rejected frame
  // throws where it arrived — which is what `onInvalid: 'throw'` promises.
  const dispatch = (msg: AnyMsg) => {
    // One meta object per frame, so every listener sees the same thing whether
    // or not the sender attached anything.
    const meta = msg.meta ?? {}
    for (const l of [...raw]) fire(l, msg, meta)
    if (msg.re === undefined) {
      const set = listeners.get(msg.kind)
      const d = spec[msg.kind]
      if (!set || set.size === 0 || !d) return
      // One Call/Subscription per frame, shared by every listener.
      const inc = wrap(msg.kind, d.mode, msg, meta)
      for (const l of [...set]) fire(l, inc, meta)
      return
    }
    if (msg.id === undefined) return
    if (msg.re === 'stop') {
      stops.get(key(msg.kind, msg.id))?.()
      return
    }
    const p = pending.get(msg.id)
    if (p && p.kind === msg.kind) p.handle(msg)
  }

  const shutdown = (error?: unknown) => {
    if (closed) return
    closed = { error }
    for (const p of [...pending.values()]) p.close(error)
    pending.clear()
    const reason = error === undefined ? new Error('channel closed') : error
    for (const abort of [...stops.values()]) abort(reason)
    stops.clear()
    const all = [...raw, ...[...listeners.values()].flatMap((s) => [...s])]
    raw.clear()
    listeners.clear()
    if (off) {
      const u = off
      off = null
      u()
    }
    // User code last, with state already consistent.
    for (const l of all) l.onClose?.(error)
  }

  const subscribe = (
    set: Set<Listener>,
    sink: (v: unknown, meta: Meta) => void,
    opts: ListenOptions,
    onEmpty?: () => void,
  ): Unsub => {
    if (opts.signal?.aborted) return noop
    if (closed) {
      opts.onClose?.(closed.error)
      return noop
    }
    const l: Listener = { sink, onClose: opts.onClose }
    set.add(l)
    const release = acquire()
    let done = false
    const off = () => {
      if (done) return
      done = true
      opts.signal?.removeEventListener('abort', off)
      set.delete(l)
      if (set.size === 0) onEmpty?.()
      release()
    }
    opts.signal?.addEventListener('abort', off, { once: true })
    return off
  }

  const on =
    (kind: string) =>
    (fn: (incoming: unknown, meta: Meta) => void, opts: ListenOptions = {}): Unsub => {
      let set = listeners.get(kind)
      if (!set) listeners.set(kind, (set = new Set()))
      const mine = set
      return subscribe(mine, fn, opts, () => {
        if (listeners.get(kind) === mine) listeners.delete(kind)
      })
    }

  const listen = (next: (value: unknown) => void, opts: ListenOptions = {}): Unsub => subscribe(raw, next, opts)

  /** Build what an `on.X` listener receives: the payload, and for requests the means to answer. */
  const wrap = (kind: string, mode: Mode, msg: AnyMsg, meta: Meta): unknown => {
    if (mode === 'none') return msg.payload
    const id = msg.id as string // the codec guaranteed it
    const k = key(kind, id)
    const ac = new AbortController()
    // Stay attached to hear `stop` for as long as this request is open.
    const release = acquire()
    let open = true
    const finish = () => {
      if (!open) return
      open = false
      stops.delete(k)
      release()
    }
    stops.set(k, (reason) => {
      // Nothing may go out for this id once the requester has left.
      finish()
      ac.abort(reason)
    })
    const reply = (frame: { re: Re; payload?: unknown }, out?: Meta) => {
      if (!open) return
      finish()
      send(stamp({ kind, id, ...frame }, out))
    }
    const fail = (e: unknown, out?: Meta) => reply({ re: 'err', payload: toWire(e) }, out)
    if (mode === 'one') {
      const call: Call<unknown, unknown> = {
        payload: msg.payload,
        meta,
        signal: ac.signal,
        respond: (value, out) => reply({ re: 'ok', payload: value }, out),
        fail,
      }
      return call
    }
    const sub: Subscription<unknown, unknown> = {
      payload: msg.payload,
      meta,
      signal: ac.signal,
      next: (value, out) => {
        if (open) send(stamp({ kind, id, re: 'next', payload: value }, out))
      },
      end: (out) => reply({ re: 'end' }, out),
      fail,
    }
    return sub
  }

  // -- outbound --------------------------------------------------------------

  const notify =
    (kind: string) =>
    (...args: unknown[]) => {
      const [payload, opts] = split(args)
      send(stamp({ kind, payload }, opts?.meta))
    }

  const call =
    (kind: string) =>
    (...args: unknown[]) => {
      const [payload, opts] = split(args)
      const signal = opts?.signal
      return new Promise<unknown>((resolve, reject) => {
        if (signal?.aborted) {
          reject(signal.reason)
          return
        }
        if (closed) {
          reject(closedError(kind, closed.error))
          return
        }
        const id = String(++seq)
        let release: Unsub = noop
        let done = false
        // `pending` is cleared before anything else, so a racing response is ignored.
        const settle = (fn: () => void) => {
          if (done) return
          done = true
          pending.delete(id)
          signal?.removeEventListener('abort', onAbort)
          release()
          fn()
        }
        const onAbort = () =>
          settle(() => {
            send({ kind, id, re: 'stop' })
            reject(signal?.reason)
          })
        pending.set(id, {
          kind,
          handle: (msg) => {
            if (msg.re === 'ok') settle(() => resolve(msg.payload))
            else if (msg.re === 'err') settle(() => reject(fromWire(msg.payload)))
            else if (msg.re === 'end') settle(() => reject(new Error(`${kind}: the responder ended without replying`)))
            // `next` on a single-reply call is a peer bug: ignore it and keep waiting.
          },
          close: (error) => settle(() => reject(closedError(kind, error))),
        })
        // Attaching may report an already-closed port synchronously, which settles this call.
        release = acquire()
        if (done) {
          release()
          return
        }
        signal?.addEventListener('abort', onAbort, { once: true })
        try {
          send(stamp({ kind, payload, id }, opts?.meta))
        } catch (e) {
          // e.g. a DataCloneError from a raw postMessage port
          settle(() => reject(e))
        }
      })
    }

  /**
   * Lazy: the request goes out on the first pull, mirroring "send on
   * subscribe". Values queue between pulls; leaving early sends `stop`.
   */
  const request =
    (kind: string) =>
    (...args: unknown[]): AsyncIterableIterator<unknown> => {
      const [payload, opts] = split(args)
      const signal = opts?.signal
      let id = ''
      let live = false
      let release: Unsub = noop
      const teardown = () => {
        if (!live) return
        live = false
        pending.delete(id)
        signal?.removeEventListener('abort', onAbort)
        release()
      }
      const q = queue<unknown>({
        // The consumer left first: `return()`, `throw()`, or an abort.
        onReturn: () => {
          if (!live) return
          teardown()
          send({ kind, id, re: 'stop' })
        },
      })
      // Rejects the pending pull with the reason; its own promise is not for us.
      const onAbort = () => void q.iterator.throw(signal?.reason).catch(noop)
      const start = () => {
        if (signal?.aborted) {
          onAbort()
          return
        }
        if (closed) {
          if (closed.error === undefined) q.end()
          else q.fail(closed.error)
          return
        }
        id = String(++seq)
        live = true
        pending.set(id, {
          kind,
          handle: (msg) => {
            if (msg.re === 'next') q.push(msg.payload)
            else if (msg.re === 'ok') {
              // A single reply closes a stream too.
              q.push(msg.payload)
              teardown()
              q.end()
            } else if (msg.re === 'end') {
              teardown()
              q.end()
            } else if (msg.re === 'err') {
              teardown()
              q.fail(fromWire(msg.payload))
            }
          },
          close: (error) => {
            teardown()
            if (error === undefined) q.end()
            else q.fail(error)
          },
        })
        release = acquire()
        if (!live) {
          release()
          return
        }
        signal?.addEventListener('abort', onAbort, { once: true })
        try {
          send(stamp({ kind, payload, id }, opts?.meta))
        } catch (e) {
          teardown()
          q.fail(e)
        }
      }
      let started = false
      const it: AsyncIterableIterator<unknown> = {
        [Symbol.asyncIterator]() {
          return it
        },
        next: () => {
          if (!started) {
            started = true
            start()
          }
          return q.iterator.next()
        },
        return: (value?: unknown) => q.iterator.return(value),
        throw: (error?: unknown) => q.iterator.throw(error),
      }
      return it
    }

  // -- serve -----------------------------------------------------------------

  const api: Record<string, unknown> = {}
  const onRec: Record<string, unknown> = {}
  for (const [kind, d] of Object.entries(spec) as [string, Desc][]) {
    if (allows(d.dir, side))
      api[kind] = d.mode === 'one' ? call(kind) : d.mode === 'many' ? request(kind) : notify(kind)
    if (allows(d.dir, flip(side))) onRec[kind] = on(kind)
  }

  const serve = (handlers: Record<string, unknown>): Unsub => {
    const offs: Unsub[] = []
    for (const [kind, handler] of Object.entries(handlers)) {
      if (typeof handler !== 'function' || !Object.hasOwn(onRec, kind)) continue
      const mode = (spec[kind] as Desc).mode
      const h = handler as Handler
      offs.push(
        on(kind)((inc, meta) => {
          if (mode === 'none') (h as (payload: unknown, ctx: Context) => void)(inc, { meta })
          else if (mode === 'one') void answer(inc as Call<unknown, unknown>, h)
          else void pump(inc as Subscription<unknown, unknown>, h)
        }),
      )
    }
    return () => {
      for (const u of offs.splice(0)) u()
    }
  }

  return Object.assign({ send, listen }, api, { on: onRec, serve }) as unknown as Sided<T, D>
}

// -- handler drivers ----------------------------------------------------------
//
// Neither ever rejects: every path is caught, and anything the handler produces
// after the requester has gone is dropped rather than sent.

const answer = async (inc: Call<unknown, unknown>, handler: Handler) => {
  if (inc.signal.aborted) return
  try {
    inc.respond(await handler(inc.payload, { signal: inc.signal, meta: inc.meta }))
  } catch (e) {
    inc.fail(e)
  }
}

type AnyIterator = AsyncIterator<unknown> | Iterator<unknown>

const iterate = (source: unknown): AnyIterator => {
  const s = source as
    | { [Symbol.asyncIterator]?: () => AsyncIterator<unknown>; [Symbol.iterator]?: () => Iterator<unknown> }
    | null
    | undefined
  const async = s?.[Symbol.asyncIterator]
  if (typeof async === 'function') return async.call(s)
  const sync = s?.[Symbol.iterator]
  if (typeof sync === 'function') return sync.call(s)
  throw new TypeError('a streaming handler must return an AsyncIterable or an Iterable')
}

/** A teardown that throws must not escape into whatever aborted the request. */
const detach = (off: Unsub) => {
  try {
    off()
  } catch {
    // a teardown that threw
  }
}

const retire = (it: AnyIterator) => {
  try {
    void Promise.resolve(it.return?.()).catch(noop)
  } catch {
    // a synchronous return() that threw
  }
}

/**
 * Resolves what the handler returned, then hands it to the driver that suits
 * it. A function is a {@link Source} to subscribe to; anything else is iterated.
 */
const pump = async (inc: Subscription<unknown, unknown>, handler: Handler) => {
  if (inc.signal.aborted) return
  let source: unknown
  try {
    source = await handler(inc.payload, { signal: inc.signal, meta: inc.meta })
  } catch (e) {
    if (!inc.signal.aborted) inc.fail(e)
    return
  }
  // Nothing has been started yet, so an abort while the handler ran needs no
  // teardown. Every driver below registers its own listener from here on with
  // no await in between, so a `stop` cannot slip through the gap.
  if (inc.signal.aborted) return
  if (typeof source === 'function') return drive(inc, source as Source<unknown>)
  return drain(inc, source)
}

/** Pulls an iterable dry, one value per pull, until it ends or the requester leaves. */
const drain = async (inc: Subscription<unknown, unknown>, source: unknown) => {
  let it: AnyIterator
  try {
    it = iterate(source)
  } catch (e) {
    inc.fail(e)
    return
  }
  const onAbort = () => retire(it)
  inc.signal.addEventListener('abort', onAbort, { once: true })
  try {
    for (;;) {
      const r = await it.next()
      // A value that landed after `stop`: `onAbort` has already queued `return()`.
      if (inc.signal.aborted) return
      if (r.done) {
        inc.end()
        return
      }
      inc.next(r.value)
    }
  } catch (e) {
    // A throw after abort is the source reacting to `return()`.
    if (!inc.signal.aborted) inc.fail(e)
  } finally {
    inc.signal.removeEventListener('abort', onAbort)
  }
}

/**
 * Subscribes to a push source and forwards what it pushes.
 *
 * A source may deliver — and close — synchronously from inside the subscribe
 * call, before its teardown is in hand, so teardown is recorded rather than run
 * and performed once there is something to run.
 */
const drive = (inc: Subscription<unknown, unknown>, source: Source<unknown>) => {
  let off: Unsub | null = null
  let live = true
  const release = () => {
    if (!live) return
    live = false
    inc.signal.removeEventListener('abort', onAbort)
    if (off) detach(off)
  }
  const onAbort = () => release()
  inc.signal.addEventListener('abort', onAbort, { once: true })
  try {
    off = source(
      (value) => {
        if (live) inc.next(value)
      },
      (error) => {
        if (!live) return
        release()
        if (error === undefined) inc.end()
        else inc.fail(error)
      },
    )
    // It closed, or the requester left, before `off` existed.
    if (!live) detach(off)
  } catch (e) {
    release()
    // A throw after abort is the source reacting to its teardown.
    if (!inc.signal.aborted) inc.fail(e)
  }
}

// ---------------------------------------------------------------------------
// Mounting several protocols on one transport
// ---------------------------------------------------------------------------

type AnyProtocol = Protocol<any, any>
type AnyChannel = Channel<any, any>
/** A {@link pair} or a {@link symmetric}: both frame by name, so both can share a transport. */
type Mountable = AnyProtocol | AnyChannel

export type Mounted<Ps extends Record<string, Mountable>, D extends Dir> = {
  [K in keyof Ps]: Ps[K] extends AnyChannel
    ? Ps[K] extends Channel<infer T, any>
      ? Peer<T>
      : never
    : Ps[K] extends Protocol<infer T, any>
      ? Sided<T, D>
      : never
}

/** A channel has one end, so `left` and `right` hand back the same thing. */
const instance = (p: Mountable, side: 'left' | 'right', transport: Port<any, any>) =>
  'connect' in p ? p.connect(transport) : p[side](transport)

/**
 * Runs several protocols over one transport. Each protocol's codec drops frames
 * whose `_t` does not match, and each attaches to the transport only while it
 * has something to hear.
 *
 * @example
 * ```ts
 * const app = mount({ chat, presence, files: files.at('files.v2') })
 * const l = app.left(transport)
 * l.chat.log('hi')
 * ```
 */
export const mount = <Ps extends Record<string, Mountable>>(protocols: Ps) => ({
  left: (transport: Port<any, any>) =>
    Object.fromEntries(
      Object.entries(protocols).map(([k, p]) => [k, instance(p, 'left', transport)]),
    ) as unknown as Mounted<Ps, 'l'>,
  right: (transport: Port<any, any>) =>
    Object.fromEntries(
      Object.entries(protocols).map(([k, p]) => [k, instance(p, 'right', transport)]),
    ) as unknown as Mounted<Ps, 'r'>,
})
