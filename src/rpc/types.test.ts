/**
 * Inference assertions, including negative cases. Checked by `tsc`, not
 * collected by vitest.
 */
import { test } from 'vitest'
import type { Link, Session } from '../index.ts'
import {
  both,
  symmetric,
  mount,
  pair,
  recv,
  send,
  stream,
  type CallOptions,
  type ListenOptions,
  type Meta,
  type Port,
  type Frame,
  type RecvMsg,
  type SendMsg,
  type SendOptions,
  type Source,
  type Unsub,
  type Validate,
} from './index.ts'

test('types', () => {})
// --- assertion helpers ------------------------------------------------------

type Eq<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false
const expect = <A>() => ({
  is: <B>(..._: Eq<A, B> extends true ? [] : [never]) => {},
})

// --- fixture ----------------------------------------------------------------

interface User {
  id: string
  name: string
}
interface Row {
  n: number
}
interface ServerEvent {
  at: number
}

// Deliberately no `as const`: the literal directions live in the return types
// of send/recv/both, so nothing needs widening protection.
const api = pair('api', {
  log: send<string>(),
  push: recv<ServerEvent>(),
  ready: both<void>(),
  getUser: send<{ id: string }>().reply<User>(),
  watch: send<{ q: string }>().stream<Row>(),
  ping: recv<void>().reply<number>(),
  shorthand: send<string>().reply<boolean>(),
  shortStream: stream<string, Row>(),
})

const transport: Port<Frame, Frame> = {
  send: () => {},
  listen: () => () => {},
}
const l = api.left(transport)
const r = api.right(transport)

// --- generated call surface: left ------------------------------------------

expect<typeof l.log>().is<(payload: string, opts?: SendOptions) => void>()
expect<typeof l.ready>().is<(opts?: SendOptions) => void>() // void payload takes no argument
expect<typeof l.getUser>().is<(payload: { id: string }, opts?: CallOptions) => Promise<User>>()
expect<typeof l.watch>().is<(payload: { q: string }, opts?: CallOptions) => AsyncIterableIterator<Row>>()
expect<typeof l.shorthand>().is<(payload: string, opts?: CallOptions) => Promise<boolean>>()
expect<typeof l.shortStream>().is<(payload: string, opts?: CallOptions) => AsyncIterableIterator<Row>>()

// @ts-expect-error `push` is right-originated; the left side cannot send it
l.push

// @ts-expect-error `ping` is right-originated
l.ping

// --- generated call surface: right -----------------------------------------

expect<typeof r.push>().is<(payload: ServerEvent, opts?: SendOptions) => void>()
expect<typeof r.ping>().is<(opts?: CallOptions) => Promise<number>>() // void payload: options only
expect<typeof r.ready>().is<(opts?: SendOptions) => void>() // `both` reaches both sides

// @ts-expect-error `log` is left-originated
r.log

// @ts-expect-error `watch` is left-originated
r.watch

// --- inbound listeners ------------------------------------------------------

// every listener is handed the frame's meta alongside the message
expect<typeof r.on.log>().is<(fn: (incoming: string, meta: Meta) => void, opts?: ListenOptions) => Unsub>()
expect<typeof l.on.push>().is<(fn: (incoming: ServerEvent, meta: Meta) => void, opts?: ListenOptions) => Unsub>()
expect<typeof l.on.ready>().is<(fn: (incoming: void, meta: Meta) => void, opts?: ListenOptions) => Unsub>()

// a request carries the means to answer it, a signal for when the asker leaves,
// and whatever meta the asker attached
expect<typeof r.on.getUser>().is<
  (
    fn: (
      incoming: {
        payload: { id: string }
        meta: Meta
        respond: (value: User, meta?: Meta) => void
        fail: (error: unknown, meta?: Meta) => void
        signal: AbortSignal
      },
      meta: Meta,
    ) => void,
    opts?: ListenOptions,
  ) => Unsub
>()

expect<typeof r.on.watch>().is<
  (
    fn: (
      incoming: {
        payload: { q: string }
        meta: Meta
        next: (value: Row, meta?: Meta) => void
        end: (meta?: Meta) => void
        fail: (error: unknown, meta?: Meta) => void
        signal: AbortSignal
      },
      meta: Meta,
    ) => void,
    opts?: ListenOptions,
  ) => Unsub
>()

// @ts-expect-error the left side never receives `log`; it sends it
l.on.log

// --- handlers ---------------------------------------------------------------

r.serve({
  log: (s) => expect<typeof s>().is<string>(),
  getUser: async ({ id }, ctx) => {
    expect<typeof ctx.signal>().is<AbortSignal>()
    return { id, name: 'x' }
  },
  ready: () => {},
})

declare const rows: AsyncIterable<Row>
r.serve({ watch: () => rows })
r.serve({ watch: async () => rows })
r.serve({ watch: () => [{ n: 1 }] })
r.serve({
  watch: async function* (_q, { signal }) {
    expect<typeof signal>().is<AbortSignal>()
    yield { n: 1 }
  },
})

// @ts-expect-error handler must return the declared response type
r.serve({ getUser: async () => ({ wrong: true }) })

// @ts-expect-error a streaming handler must return an iterable, not a value
r.serve({ watch: () => ({ n: 1 }) })

// @ts-expect-error the right side cannot serve its own outbound message
r.serve({ push: () => {} })

const text = pair('text', { lines: send<void>().stream<string>() })
// @ts-expect-error a bare string is iterable, but it is not a stream of strings
text.right(transport).serve({ lines: () => 'abc' })

// --- streaming from a push source -------------------------------------------

// a streaming handler may return a subscribe function instead of an iterable
r.serve({ watch: () => (next, close) => (next({ n: 1 }), close(), () => {}) })
r.serve({ watch: async () => (next) => topic.listen((f) => next({ n: Number(f.kind) })) })
r.serve({
  watch: () => (next) => {
    expect<typeof next>().is<(value: Row) => void>()
    return () => {}
  },
})

// a port's listener is one: `onClose(error?)` is exactly `close`
const rowPort: Port<never, Row> = {} as any
r.serve({ watch: () => (next, close) => rowPort.listen(next, { onClose: close }) })

// @ts-expect-error the source must push what the stream declares
r.serve({ watch: () => (next: (v: string) => void) => () => void next })

// @ts-expect-error a source must return its teardown
r.serve({ watch: () => () => 'not an unsub' })

// @ts-expect-error the handler returns a source; it is not itself one
r.serve({ watch: (next: (v: Row) => void) => () => void next })

expect<Source<Row>>().is<(next: (value: Row) => void, close: (error?: unknown) => void) => Unsub>()

// --- calls and options ------------------------------------------------------

void l.getUser({ id: '1' }, { signal: new AbortController().signal })
void r.ping()
void r.ping({ signal: undefined })

// @ts-expect-error unknown option
void l.getUser({ id: '1' }, { bogus: 1 })

// --- meta -------------------------------------------------------------------

// anything may carry meta: a notification, a call, a stream, a void message
l.log('x', { meta: { trace: 't' } })
l.ready({ meta: { trace: 't' } })
void l.getUser({ id: '1' }, { meta: { trace: 't' } })
void l.watch({ q: 'x' }, { meta: { trace: 't' }, signal: new AbortController().signal })

// @ts-expect-error notifications have nothing to cancel
l.log('x', { signal: new AbortController().signal })

// @ts-expect-error meta is a bag of keys, not a scalar
l.log('x', { meta: 1 })

// @ts-expect-error a void message takes no payload
l.ready(1)

const consume = async () => {
  for await (const row of l.watch({ q: 'x' })) expect<typeof row>().is<Row>()
}
void consume

// --- a side is a port; links and topics are ports ---------------------------

const _asPort: Port<SendMsg<typeof api.spec, 'l'>, RecvMsg<typeof api.spec, 'l'>> = l
void _asPort

const link: Link<Frame> = {} as any
const topic: Session.Topic<Frame, Frame> = {} as any
void api.left(link)
void api.left(topic)

// --- raw message unions -----------------------------------------------------

// the left side puts requests, replies to right-originated calls, and
// cancellations — of streams and of single calls — on the wire
type LeftWire = SendMsg<typeof api.spec, 'l'>
const _asks: LeftWire = { kind: 'getUser', payload: { id: '1' } }
const _answers: LeftWire = { kind: 'ping', id: '1', re: 'ok', payload: 7 }
const _stops: LeftWire = { kind: 'watch', id: '1', re: 'stop' }
const _stopCall: LeftWire = { kind: 'getUser', id: '1', re: 'stop' }
void [_asks, _answers, _stops, _stopCall]

// @ts-expect-error `push` is not left-originated
const _bad: LeftWire = { kind: 'push', payload: { at: 0 } }
void _bad

// @ts-expect-error `log` expects no reply, so there is no reply frame for it
const _noReply: LeftWire = { kind: 'log', id: '1', re: 'ok', payload: 'x' }
void _noReply

// --- validators declare the type -------------------------------------------

/** Hand-rolled Standard Schemas, so the assertions below also run. */
const schema = <T>(vendor: string): Validate.StandardSchemaV1<unknown, T> => ({
  '~standard': { version: 1, vendor, validate: (value) => ({ value: value as T }) },
})
const zString = schema<string>('test')
const zUser = schema<User>('test')
const isRow = (p: unknown) => (p as Row | undefined) ?? undefined

const checked = pair('checked', {
  log: send(zString),
  getUser: send(zString).reply(zUser),
  watch: send(zString).stream(isRow),
  mixed: send<{ id: string }>().reply(zUser),
  half: send(zString).reply<User>(),
})
const cl = checked.left(transport)

expect<typeof cl.log>().is<(payload: string, opts?: SendOptions) => void>()
expect<typeof cl.getUser>().is<(payload: string, opts?: CallOptions) => Promise<User>>()
expect<typeof cl.watch>().is<(payload: string, opts?: CallOptions) => AsyncIterableIterator<Row>>()
expect<typeof cl.mixed>().is<(payload: { id: string }, opts?: CallOptions) => Promise<User>>()
expect<typeof cl.half>().is<(payload: string, opts?: CallOptions) => Promise<User>>()

// a validator narrows the handler's payload too
checked.right(transport).serve({
  log: (s) => expect<typeof s>().is<string>(),
  getUser: async (q) => {
    expect<typeof q>().is<string>()
    return { id: 'x', name: 'y' }
  },
})

// @ts-expect-error the response must match what the validator narrows to
checked.right(transport).serve({ getUser: async () => ({ wrong: true }) })

// --- reserved names ---------------------------------------------------------

try {
  pair('bad', {
    // @ts-expect-error `send` collides with a port member
    send: send<string>(),
  })
} catch {}

try {
  pair('bad2', {
    // @ts-expect-error `listen` collides with a port member
    listen: send<string>(),
  })
} catch {}

// a side is a plain object, so these no longer collide with anything
void pair('ok', { pipe: send<string>(), call: send<void>(), apply: recv<void>(), recv: both<void>() })

// --- channels: one spec, one kind of end ------------------------------------

const room = symmetric('room', {
  chat: both<string>(),
  ready: both<void>(),
  ping: both<void>().reply<number>(),
  history: both<{ q: string }>().stream<Row>(),
})

const p1 = room.connect(transport)
const p2 = room.connect(transport)

// every instance has the same type, so there is nothing to pick between
expect<typeof p1>().is<typeof p2>()

expect<typeof p1.chat>().is<(payload: string, opts?: SendOptions) => void>()
expect<typeof p1.ready>().is<(opts?: SendOptions) => void>()
expect<typeof p1.ping>().is<(opts?: CallOptions) => Promise<number>>()
expect<typeof p1.history>().is<(payload: { q: string }, opts?: CallOptions) => AsyncIterableIterator<Row>>()

// and receives everything it can send
expect<typeof p1.on.chat>().is<(fn: (incoming: string, meta: Meta) => void, opts?: ListenOptions) => Unsub>()
expect<typeof p1.on.ping>().is<
  (
    fn: (
      incoming: {
        payload: void
        meta: Meta
        respond: (value: number, meta?: Meta) => void
        fail: (error: unknown, meta?: Meta) => void
        signal: AbortSignal
      },
      meta: Meta,
    ) => void,
    opts?: ListenOptions,
  ) => Unsub
>()

p1.serve({
  chat: (s) => expect<typeof s>().is<string>(),
  ping: () => 1,
  history: function* () {
    yield { n: 1 }
  },
})

// @ts-expect-error a channel has no far side to hear from
p1.serve({ nope: () => {} })

try {
  symmetric('one-way', {
    // @ts-expect-error a channel's messages must be declared with both()
    log: send<string>(),
    // @ts-expect-error same for recv()
    push: recv<ServerEvent>(),
    fine: both<string>(),
  })
} catch {}

try {
  symmetric('bad', {
    // @ts-expect-error `send` collides with a port member
    send: both<string>(),
  })
} catch {}

// a channel is a port, renames, and mounts alongside protocols
const _peerAsPort: Port<SendMsg<typeof room.spec, 'l'>, RecvMsg<typeof room.spec, 'l'>> = p1
void _peerAsPort
expect<ReturnType<ReturnType<typeof room.at>['connect']>>().is<typeof p1>()

const mixed = mount({ api, room })
expect<ReturnType<typeof mixed.left>['room']>().is<typeof p1>()
expect<ReturnType<typeof mixed.right>['room']>().is<typeof p1>()
expect<ReturnType<typeof mixed.left>['api']>().is<typeof l>()

// --- composition ------------------------------------------------------------

const chat = pair('chat', { say: send<string>(), heard: recv<string>() })
const app = mount({ api, chat, chat2: chat.at('chat.v2') })
const both_ = app.left(transport)

expect<typeof both_.chat.say>().is<(payload: string, opts?: SendOptions) => void>()
expect<typeof both_.api.getUser>().is<(payload: { id: string }, opts?: CallOptions) => Promise<User>>()
expect<typeof both_.chat2.say>().is<(payload: string, opts?: SendOptions) => void>()

// spreading a spec extends a protocol
const v2 = pair('api.v2', { ...api.spec, cancel: send<string>() })
expect<Parameters<ReturnType<typeof v2.left>['cancel']>[0]>().is<string>()
expect<ReturnType<ReturnType<typeof v2.left>['getUser']>>().is<Promise<User>>()
