import type { ListenOptions, Unsub } from '../index.ts'
import type { Validate } from '../core/validate.ts'

// ---------------------------------------------------------------------------
// Descriptors
// ---------------------------------------------------------------------------

/** Which side originates a message. `'b'` means either side may. */
export type Dir = 'l' | 'r' | 'b'

/** How many responses a message expects. */
export type Mode = 'none' | 'one' | 'many'

/**
 * Arbitrary out-of-band data riding alongside a payload — a trace id, a token,
 * a clock reading, whatever the two ends agree on.
 *
 * It is deliberately outside the spec and outside validation: a descriptor says
 * what a message *means*, and meta says something about the circumstances it
 * was sent in. Frames carry it only when there is some, so a protocol that
 * never sets it looks exactly as it did before.
 */
export type Meta = Record<string, unknown>

/**
 * One entry in a {@link Spec}. Carries its direction and reply arity at the
 * type level; `_q` and `_r` are phantom fields holding the request and
 * response payload types.
 *
 * Each step takes either a type argument or a validator. A validator both
 * narrows the payload at the decode boundary and *is* the type declaration, so
 * the two cannot drift apart.
 *
 * @typeParam D - Originating side
 * @typeParam Q - Request payload
 * @typeParam R - Response payload (`never` when the message expects no reply)
 * @typeParam M - Reply arity
 */
export interface Desc<D extends Dir = Dir, Q = any, R = any, M extends Mode = Mode> {
  readonly dir: D
  readonly mode: M
  /** Narrows the request payload on decode. */
  readonly req?: Validate | undefined
  /** Narrows the response payload on decode. */
  readonly res?: Validate | undefined
  readonly _q: Q
  readonly _r: R
  /** Expect exactly one response of type `T`. */
  reply<T>(): Desc<D, Q, T, 'one'>
  /** Expect exactly one response, of whatever `schema` narrows to. */
  reply<S extends Validate>(schema: S): Desc<D, Q, Validate.InferOutput<S>, 'one'>
  /** Expect zero or more responses of type `T`, then completion. */
  stream<T>(): Desc<D, Q, T, 'many'>
  /** Expect zero or more responses, of whatever `schema` narrows to, then completion. */
  stream<S extends Validate>(schema: S): Desc<D, Q, Validate.InferOutput<S>, 'many'>
}

const desc = <D extends Dir, Q, R, M extends Mode>(
  dir: D,
  mode: M,
  req?: Validate,
  res?: Validate,
): Desc<D, Q, R, M> => ({
  dir,
  mode,
  req,
  res,
  _q: undefined as never,
  _r: undefined as never,
  reply: (schema?: Validate) => desc<D, Q, any, 'one'>(dir, 'one', req, schema ?? res),
  stream: (schema?: Validate) => desc<D, Q, any, 'many'>(dir, 'many', req, schema ?? res),
})

/** Left sends, right receives. */
export function send<Q = void>(): Desc<'l', Q, never, 'none'>
export function send<S extends Validate>(schema: S): Desc<'l', Validate.InferOutput<S>, never, 'none'>
export function send(schema?: Validate) {
  return desc<'l', unknown, never, 'none'>('l', 'none', schema)
}

/** Right sends, left receives. */
export function recv<Q = void>(): Desc<'r', Q, never, 'none'>
export function recv<S extends Validate>(schema: S): Desc<'r', Validate.InferOutput<S>, never, 'none'>
export function recv(schema?: Validate) {
  return desc<'r', unknown, never, 'none'>('r', 'none', schema)
}

/** Either side may send. */
export function both<Q = void>(): Desc<'b', Q, never, 'none'>
export function both<S extends Validate>(schema: S): Desc<'b', Validate.InferOutput<S>, never, 'none'>
export function both(schema?: Validate) {
  return desc<'b', unknown, never, 'none'>('b', 'none', schema)
}

/** Shorthand for `send<Q>().stream<R>()`. Pass validators through `send().stream()` instead. */
export const stream = <Q = void, R = void>() => send<Q>().stream<R>()

/** A protocol: one flat map of message name to descriptor. */
export type Spec = Record<string, Desc>

// ---------------------------------------------------------------------------
// Reserved names
//
// The generated call surface sits on the same object as the port, so a message
// may not be named after one of its members. `then` is reserved so a side can
// never be mistaken for a thenable. This surfaces as an error on the offending
// key rather than a mystery elsewhere.
// ---------------------------------------------------------------------------

export type Reserved = 'send' | 'listen' | 'on' | 'serve' | 'then'

export type CheckKeys<T> =
  Extract<keyof T, Reserved> extends never
    ? unknown
    : { [K in Extract<keyof T, Reserved>]: 'This message name is reserved by the channel; rename it.' }

/** Message names declared with `send()` or `recv()` rather than `both()`. */
type OneWay<T> = { [K in keyof T]: T[K] extends Desc<infer D> ? ('b' extends D ? never : K) : K }[keyof T]

/**
 * Every end of a {@link symmetric} channel is the same end, so a one-directional
 * message has no meaning there. This surfaces as an error on the offending key.
 */
export type CheckDirs<T> =
  OneWay<T> extends never
    ? unknown
    : { [K in OneWay<T>]: 'A channel has one kind of end; declare this message with both().' }

// ---------------------------------------------------------------------------
// Direction algebra
// ---------------------------------------------------------------------------

export type Flip<D extends Dir> = D extends 'l' ? 'r' : 'l'

/** Message names side `D` may originate. */
export type Sends<T extends Spec, D extends Dir> = {
  [K in keyof T]: T[K]['dir'] extends D | 'b' ? K : never
}[keyof T]

/** Message names side `D` receives — everything the other side may originate. */
export type Recvs<T extends Spec, D extends Dir> = Sends<T, Flip<D>>

/** Message names side `D` originates that expect a reply. */
export type Asks<T extends Spec, D extends Dir> = {
  [K in Sends<T, D>]: T[K]['mode'] extends 'none' ? never : K
}[Sends<T, D>]

// ---------------------------------------------------------------------------
// Wire-level message unions
// ---------------------------------------------------------------------------

/** Marks a frame as a response rather than a request. */
export type Re = 'next' | 'ok' | 'err' | 'end' | 'stop'

export type Request<T extends Spec, K extends keyof T> = {
  kind: K
  payload: T[K]['_q']
  /** Correlation id. Present when the message expects a reply. */
  id?: string
  /** Never set on a request; declared so a raw listener can narrow on it. */
  re?: undefined
  /** Whatever the sender attached. Absent when it attached nothing. */
  meta?: Meta
}

export type Response<T extends Spec, K extends keyof T> =
  | { kind: K; id: string; re: 'next' | 'ok'; payload: T[K]['_r']; meta?: Meta }
  | { kind: K; id: string; re: 'err'; payload: unknown; meta?: Meta }
  | { kind: K; id: string; re: 'end'; meta?: Meta }

/** Cancellation, sent by the requester back towards the responder. */
export type Stop<K> = { kind: K; id: string; re: 'stop'; meta?: Meta }

type Requests<T extends Spec, D extends Dir> = { [K in Sends<T, D>]: Request<T, K> }[Sends<T, D>]
type Responses<T extends Spec, D extends Dir> = { [K in Asks<T, Flip<D>>]: Response<T, K> }[Asks<T, Flip<D>>]
/** Any message expecting a reply — one or many — may be stopped by its requester. */
type Stops<T extends Spec, D extends Dir> = { [K in Asks<T, D>]: Stop<K> }[Asks<T, D>]

/** Everything side `D` may put on the wire. */
export type SendMsg<T extends Spec, D extends Dir> = Requests<T, D> | Responses<T, D> | Stops<T, D>
/** Everything side `D` may read off the wire. */
export type RecvMsg<T extends Spec, D extends Dir> = SendMsg<T, Flip<D>>

// ---------------------------------------------------------------------------
// Generated call surface
// ---------------------------------------------------------------------------

/** Drops the argument entirely for `void` payloads, so `ch.ready()` typechecks. */
type Args<Q> = [Q] extends [void] ? [] : [payload: Q]

export type Api<T extends Spec, D extends Dir> = {
  [K in Sends<T, D>]: T[K]['mode'] extends 'one'
    ? (...args: [...Args<T[K]['_q']>, opts?: CallOptions]) => Promise<T[K]['_r']>
    : T[K]['mode'] extends 'many'
      ? (...args: [...Args<T[K]['_q']>, opts?: CallOptions]) => AsyncIterableIterator<T[K]['_r']>
      : (...args: [...Args<T[K]['_q']>, opts?: SendOptions]) => void
}

/** A single request expecting one response. */
export interface Call<Q, R> {
  payload: Q
  /** What the requester attached to the request. Empty when it attached nothing. */
  meta: Meta
  respond: (value: R, meta?: Meta) => void
  fail: (error: unknown, meta?: Meta) => void
  /** Aborts when the requester gives up, or the transport closes. */
  signal: AbortSignal
}

/** A single request expecting a stream of responses. */
export interface Subscription<Q, R> {
  payload: Q
  /** What the requester attached to the request. Empty when it attached nothing. */
  meta: Meta
  next: (value: R, meta?: Meta) => void
  end: (meta?: Meta) => void
  fail: (error: unknown, meta?: Meta) => void
  /** Aborts when the requester stops listening, or the transport closes. */
  signal: AbortSignal
}

export type Incoming<T extends Spec, K extends keyof T> = T[K]['mode'] extends 'one'
  ? Call<T[K]['_q'], T[K]['_r']>
  : T[K]['mode'] extends 'many'
    ? Subscription<T[K]['_q'], T[K]['_r']>
    : T[K]['_q']

/** Per-message inbound listeners. Each returns a teardown. */
export type On<T extends Spec, D extends Dir> = {
  [K in Recvs<T, D>]: (fn: (incoming: Incoming<T, K>, meta: Meta) => void, opts?: ListenOptions) => Unsub
}

/** What every handler is given besides the payload. */
export interface Context {
  /** What the sender attached to the frame. Empty when it attached nothing. */
  meta: Meta
}

/** What a request handler is given besides the payload. */
export interface HandlerContext extends Context {
  /** Aborts when the requester gives up. Streaming handlers must observe it. */
  signal: AbortSignal
}

/**
 * A push source, shaped like a port's `listen`: subscribe with `next`, report
 * the end through `close` — an `error` when it failed, nothing when it
 * completed — and return the teardown, which runs when the requester leaves.
 *
 * Handlers that ignore `close` stream until the requester stops.
 *
 * @example
 * ```ts
 * serve({ watch: () => (next, close) => topic.listen(next, { onClose: close }) })
 * ```
 */
export type Source<R> = (next: (value: R) => void, close: (error?: unknown) => void) => Unsub

/**
 * What a streaming handler returns: anything iterable, sync or async — an
 * array, a generator, an `async function*` — or a {@link Source} to subscribe
 * to, for a push source that has no iterator. `& object` keeps a bare string
 * from passing as `Iterable<string>`.
 */
export type Streamed<R> = ((AsyncIterable<R> | Iterable<R>) & object) | Source<R>

export type Handlers<T extends Spec, D extends Dir> = {
  [K in Recvs<T, D>]?: T[K]['mode'] extends 'one'
    ? (payload: T[K]['_q'], ctx: HandlerContext) => T[K]['_r'] | Promise<T[K]['_r']>
    : T[K]['mode'] extends 'many'
      ? (payload: T[K]['_q'], ctx: HandlerContext) => Streamed<T[K]['_r']> | Promise<Streamed<T[K]['_r']>>
      : (payload: T[K]['_q'], ctx: Context) => void
}

/** What may be attached to any outbound message. */
export interface SendOptions {
  /** Rides along with the frame, untyped and unvalidated. */
  meta?: Meta | undefined
}

export interface CallOptions extends SendOptions {
  signal?: AbortSignal | undefined
}
