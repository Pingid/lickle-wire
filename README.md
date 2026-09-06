# @lickle/wire

Composable transport, pub/sub, RPC and state-replication primitives for the browser, workers and Node.
Zero runtime dependencies.

[![Build Status](https://img.shields.io/github/actions/workflow/status/Pingid/lickle-wire/test.yml?branch=main&style=flat&colorA=000000&colorB=000000)](https://github.com/Pingid/lickle-wire/actions?query=workflow:Test)
[![Build Size](https://img.shields.io/bundlephobia/minzip/@lickle/wire?label=bundle%20size&style=flat&colorA=000000&colorB=000000)](https://bundlephobia.com/result?p=@lickle/wire)
[![Version](https://img.shields.io/npm/v/@lickle/wire?style=flat&colorA=000000&colorB=000000)](https://www.npmjs.com/package/@lickle/wire)
[![Downloads](https://img.shields.io/npm/dt/@lickle/wire.svg?style=flat&colorA=000000&colorB=000000)](https://www.npmjs.com/package/@lickle/wire)

## Install

```bash
npm install @lickle/wire
```

---

## Quick Start

The hub lives in the page and the workers are its peers. Nothing is spawned until the hub serves.

```ts
// page.ts
import { Hub, persistent } from '@lickle/wire'
import { link, worker } from '@lickle/wire/browser'

type Channels = { tick: number }

const hub = Hub.create<Channels>('app', { retain: ['tick'] })

hub.serve(worker('./worker-a.ts')) // spawned here, terminated on teardown
hub.serve(persistent(() => link(new Worker('./worker-b.ts')))) // respawned on failure

const sesh = hub.local()
sesh.topic('tick').send(1)
```

Each worker joins the same bus by name, over the port it already holds:

```ts
// worker-a.ts
import { Session } from '@lickle/wire'
import { link } from '@lickle/wire/browser'

const session = Session.over<Channels>('app', link(self))
await session.ready()

session.topic('tick').listen((t, meta) => console.log(t, meta.retained ? '(replayed)' : ''))
session.topic('tick').send(2) // reaches the page and worker-b, never itself
```

Typed request/response rides one of the hub's channels. Declare the protocol once, add a channel to carry
its frames, and hand a topic to each side:

```ts
// shared.ts
import { pair, send, stream, type Frame } from '@lickle/wire/rpc'

export type Channels = { tick: number; api: Frame }

export const api = pair('api', {
  getUser: send<{ id: string }>().reply<{ id: string; name: string }>(),
  watch: stream<{ q: string }, number>(),
})
```

```ts
// worker-a.ts — the responder
api.right(session.topic('api')).serve({
  getUser: async ({ id }) => ({ id, name: `user-${id}` }),
  watch: async function* (_query, { signal }) {
    for (let n = 0; !signal.aborted; n++) {
      yield n
      await new Promise((r) => setTimeout(r, 1000))
    }
  },
})
```

```ts
// page.ts — the caller
const client = api.left(sesh.topic('api'))

const user = await client.getUser({ id: '1' })
for await (const n of client.watch({ q: 'x' })) {
  if (n > 3) break // tells the worker to stop
}
```

Everything above also runs in one process, which is how the library tests itself:

```ts
import { Hub } from '@lickle/wire'

const hub = Hub.create<Channels>('app', { retain: ['tick'] })
const a = hub.local()
const b = hub.local()
await Promise.all([a.ready(), b.ready()])

hub.publish('tick', 1)
b.topic('tick').listen((t, meta) => console.log(t, meta.retained)) // 1 true — replayed on subscribe
a.topic('tick').send(2) // 2 false — live
```

---

## Core Concepts

### Port

One contract, spoken by every layer:

```ts
interface Port<S, R = S> {
  send(value: S): void
  listen(next: (value: R) => void, opts?: ListenOptions): () => void
}

interface ListenOptions {
  signal?: AbortSignal // detach when aborted, instead of holding the teardown
  onClose?: (error?: unknown) => void // terminal: nothing more will be delivered
}
```

A link is a port. A topic is a port. Either side of an RPC protocol is a port. `link(worker)` is a
port. So RPC and replication run over any of them — a worker directly, a hub topic, or a test double —
without adapters. Anything you build that satisfies the interface plugs in the same way.

### Link

Layer 0: one end of an open duplex symmetric, with reconnection.

```ts
import { Link } from '@lickle/wire'

const [a, b] = pair<string>() // two cross-wired in-memory ends
b.listen((m) => console.log(m))
a.send('hi') // structured-cloned and delivered on a microtask, like a real transport

const link = persistent(() => connect(), {
  backoff: { base: 250, max: 30_000 },
  jitter: 0.2,
  timeout: 10_000, // give up an attempt that never delivers a first message
  maxAttempts: 0, // forever
  buffer: 64, // sends held while down
  ttl: 5_000, // ...but not for longer than this
})
link.state // 'connecting' | 'open' | 'retrying' | 'closed'
link.retryNow() // for `online` and `visibilitychange` handlers
```

An `ILink` adds `remote`, `meta`, `up`, `changed(fn)`, `closed(fn)` and `close()` to the port contract. `send`
never throws and returns `false` when a message was dropped. Inbound messages that arrive before the first
`listen` are buffered, so a greeting is never lost.

### Session and Topic

Layer 2: the client side. A session owns its _subscription intent_ — the link is disposable, the intent is
not, and it is replayed on every reconnect.

```ts
const session = Session.over<Channels>('app', link(self)) // ends when the link does
const reconnecting = Session.over<Channels>('app', persistent(connector))

await session.ready()
session.id // peer id assigned by the hub

const tick = session.topic('tick')
const off = tick.listen((n, meta) => {}) // subscribes on the first listener, unsubscribes on the last
tick.once((n) => {})
for await (const n of tick.stream({ signal })) {
}
tick.send(3)
tick.send(3, { to: peerId }) // one peer only, bypassing subscriptions

const rr = session.topic('req', 'res') // publish on one symmetric, listen on another
const direct = tick.peer(peerId) // a Port that only talks to one peer

session.on('open' | 'close' | 'error', fn)
```

`session.on('close')` is the link going down, which a persistent link recovers from. `ListenOptions.onClose`
on a topic listener is the session ending, which nothing recovers from — and which is how a pending RPC call
over a dead topic gets rejected instead of hanging.

When a hub and its sessions live in different realms and share only a declaration, `define` binds the name
and the validators once so both ends cannot drift:

```ts
// shared.ts
export const app = define<Channels>({ name: 'app', retain: ['tick'], validate })

// either realm
const hub = app.hub()
const session = app.connect(connector)
```

### Hub

Layer 3: routing, policy, retained values.

```ts
const hub = app.hub({
  retain: ['tick'], // replay the last payload to each new subscriber, flagged `meta.retained`
  validate: { tick: (p) => (typeof p === 'number' ? p : undefined) }, // narrow at the trust boundary
  policy: {
    canAccept: async (peer) => verify(peer.meta), // frames are held until this settles
    canSubscribe: (peer, channel) => true,
    canPublish: (peer, symmetric, payload) => true,
    canAddress: (from, to, channel) => true,
  },
})

hub.serve(worker('./a.ts'), worker('./b.ts')) // spawned here, terminated on teardown
hub.serve(persistent(() => link(new Worker('./c.ts')))) // reconnecting
hub.serve(handshake(self)) // peers that transfer their own port
hub.local() // the hub's own process as a peer
hub.publish('tick', 1, { to: peerId })
hub.peers // stable snapshot, safe for useSyncExternalStore
hub.onPeersChange((peers) => {})
```

A `data` frame fans out to every subscriber of its channel except the sender. An addressed frame reaches
exactly one peer, carrying the sender as `meta.from`. Policy can veto; it cannot re-route.

### Framing

Every frame on a link is an envelope tagged with the hub name and a wire version, so several hubs can share
one transport and a stale peer is reported through `onError` rather than going quiet. Payloads are untyped
on the wire and typed at the `Topic`. `Validators` narrow inbound payloads at whichever end sees them first.

---

## RPC

`@lickle/wire/rpc` derives everything from one flat map of message descriptors: direction, reply arity,
validation, the wire unions, and the call surface on both sides.

```ts
import { pair, send, recv, both, stream } from '@lickle/wire/rpc'

const api = pair('api', {
  log: send<string>(), // left → right, no reply
  push: recv<ServerEvent>(), // right → left, no reply
  ready: both<void>(), // either side
  getUser: send<{ id: string }>().reply<User>(), // one response
  watch: send<Query>().stream<Row>(), // many responses, then done
  ping: send<void>().reply<number>(),
})
```

Directions read from the left side's point of view. That is the one convention to remember.

### Both sides

```ts
const l = api.left(port) // any Port<Frame, Frame>: a link, a topic, a hub peer...
const r = api.right(port)

l.log('hello') // void
const user = await l.getUser({ id: '1' }) // Promise<User>
for await (const row of l.watch(query)) {
} // AsyncIterableIterator<Row>; `break` sends stop

r.serve({
  log: (s) => console.log(s),
  getUser: async ({ id }) => lookup(id), // value or promise
  watch: async function* (q, { signal }) {
    // anything iterable: an array, a generator, an async generator
    for (const row of rows(q)) {
      if (signal.aborted) return
      yield row
    }
  },
})
```

Only the keys a side can actually use appear on it. `l.push` and `r.watch` are type errors, not runtime
no-ops.

`serve` is sugar over `on`, which gives you each inbound message plus the means to answer it:

```ts
r.on.getUser(({ payload, respond, fail, signal }) => respond(lookup(payload.id)))
r.on.watch(({ payload, next, end, fail, signal }) => {
  const stop = subscribe(payload, next, end)
  signal.addEventListener('abort', stop)
})
```

### Streaming from a push source

A streaming handler returns anything iterable — an array, a generator, an `async function*`. When the thing
you are streaming pushes instead of being pulled, return a subscribe function and it is driven for you:

```ts
r.serve({
  watch: (q) => (next, close) => subscribe(q, next, close), // returns the teardown
})
```

The shape is a port's `listen`: push with `next`, report the end through `close` — an error when it failed,
nothing when it completed — and return the teardown, which runs when the requester stops, when the stream
ends, or when the transport closes. So a port, a topic or a link is already a source:

```ts
r.serve({ watch: () => (next, close) => topic.listen(next, { onClose: close }) })
```

A source may push and close synchronously from inside the subscribe call; its teardown still runs. Values
pushed after the requester has gone are dropped rather than sent, and a source that never closes streams
until the requester stops.

### Symmetric channels

When both ends run the same code — two workers, two frames, two peers — there is no left and right to
choose between. `symmetric()` takes a spec where every message is declared with `both()` and hands back one
constructor instead of two:

```ts
import { symmetric, both } from '@lickle/wire/rpc'

const room = symmetric('room', {
  chat: both<string>(), // either way, no reply
  ping: both<void>().reply<number>(), // either way, one response
  history: both<Query>().stream<Row>(), // either way, many
})

const a = room.connect(portA)
const b = room.connect(portB)

b.serve({ ping: () => Date.now() })
a.on.chat((line) => console.log(line))

b.chat('hi') // every instance sends, receives and serves the same messages
await a.ping()
```

`connect` is the whole difference. The wire format, `on`, `serve`, cancellation, validation, `at()`,
`with()` and `mount()` are the same as a pair's — a symmetric protocol is a pair whose two sides coincide.
`send()` or `recv()` in that spec is an error on the offending key, at compile time and at runtime.

Correlation ids are per-instance, so two peers calling at the same moment both use `1` without colliding: a
reply only ever travels back to the end that asked.

### Cancellation

Calls and streams take `{ signal }`. Aborting rejects locally with `signal.reason` and sends a `stop` frame;
the handler sees `ctx.signal` abort. Leaving a `for await` early — `break`, `return`, a throw — does the
same. Streaming handlers must observe their signal: an `async function*` that never yields to the event
loop cannot be stopped.

### What's derived

| From the spec  | You get                                                                                   |
| -------------- | ----------------------------------------------------------------------------------------- |
| `dir`          | which keys appear on each side, and which appear under `on`                               |
| `mode`         | `void` / `Promise<R>` / `AsyncIterableIterator<R>` on the caller, and the handler's shape |
| `_q` is `void` | the method takes no payload at all — `ch.ready()`, `ch.ping({ signal })`                  |
| a validator    | the payload type _and_ narrowing at the decode boundary, from one declaration             |

### Composition

Specs are plain objects, so extension is spreading; codecs compose outward; protocols share a transport:

```ts
const v2 = pair('api.v2', { ...api.spec, cancel: send<string>() })

const framed = api.with(jsonCodec).with(encryptCodec)

const app = mount({ api, chat, chat2: chat.at('chat.v2') })
const l = app.left(port)
l.chat.say('hi')
```

Each protocol's codec drops frames whose tag does not match, and each attaches to the transport only while
it has something to hear, so a side that only sends notifications holds no listener at all.

### Validation

Every step takes either a type argument or a validator. Pass a validator and it becomes the declaration:
the payload type is inferred from it, and it narrows the payload at the decode boundary, so the two cannot
drift apart.

```ts
const api = pair(
  'api',
  {
    log: send(z.string()), // payload: string
    getUser: send(QuerySchema).reply(UserSchema), // Promise<User>, both ends checked
    watch: send(QuerySchema).stream(RowSchema), // AsyncIterableIterator<Row>
    ping: send<{ id: string }>().reply(UserSchema), // mix: declared request, checked response
  },
  { onInvalid: 'drop' },
)
```

A validator is anything implementing [Standard Schema](https://standardschema.dev) — Zod, Valibot, ArkType
— or a plain function returning the narrowed value, or `undefined` to reject:

```ts
const port = (p: unknown) => (typeof p === 'number' && p > 0 ? p : undefined)
const open = pair('open', { connect: send(port) }) // payload: number
```

`onInvalid` is `'throw'` (default), `'drop'`, or a callback. The default throws from inside the transport's
delivery callback — over a link that means the link's `onError`, otherwise an unhandled error where the
frame arrived. Use `'drop'` or the callback when the far side is untrusted. Direction is enforced on decode
too: a frame claiming a message the peer may not originate is rejected the same way.

### Wire format

```ts
{ _t: 'api', kind: 'getUser', id: '3', payload: { id: '1' } }   // request
{ _t: 'api', kind: 'getUser', id: '3', re: 'ok', payload: {…} }  // response
{ _t: 'api', kind: 'watch',   id: '4', re: 'next' | 'end' | 'err' }
{ _t: 'api', kind: 'watch',   id: '4', re: 'stop' }              // cancellation
```

Errors cross as `{ name, message }` and are rehydrated into an `Error`, so they survive structured clone.

### Caveats

- **Reserved names.** A message cannot be called `send`, `listen`, `on`, `serve` or `then`; `pair()` and
  `symmetric()` reject those at compile time and at runtime.
- **Streams are lazy and single-use.** `const it = l.watch(q)` sends nothing until the first pull, and a
  second `for await` over the same iterator is already done. Values arriving faster than they are pulled
  are buffered without bound.
- **A lone argument is a payload unless it carries a live `AbortSignal`.** `r.ping({})` on a `void` message
  therefore sends `{}` rather than `undefined`; write `r.ping()` or `r.ping({ signal })`.
- **Async schemas** are rejected. Decoding happens inside the delivery callback, so there is
  nowhere to await.

---

## Replica

`@lickle/wire/replica` keeps state in step across peers over any port. A `writer` is the authority; `reader`s
follow; `readerWriter`s form a mesh and need a `merge`.

```ts
import * as replica from '@lickle/wire/replica'

const counter = replica.define({
  initial: 0,
  apply: (state, delta: number) => state + delta,
})

const w = counter.w(hub.local().topic('counter')) // any Port<Replica.Message<S, U>>
const r = counter.r(otherSession.topic('counter'))
const pinned = counter.r(third.topic('counter'), 'stable-id') // an id only when it must survive a reconnect

w.update(2)
r.get() // 2, once live
r.status // 'joining' | 'live' | 'closed'
r.listen((state) => render(state), { onClose: (error) => {} })
r.onStatus((status) => {})
r.close()
```

Joining asks for a snapshot and buffers updates until one arrives, or gives up after `join.attempts` and
goes live from `initial`. Lost updates are chased per `gap` after a grace period that lets a mere reorder
settle for free. Sequence numbers are per sender, so concurrent writes are never a protocol conflict;
convergence is the reducer's job. Pass `clock: fakeClock()` to drive every timer in tests.

---

## Browser

`@lickle/wire/browser` speaks one target type, `Link.Target`: a `Worker`, `self` inside one, a
`MessagePort`, a `BroadcastChannel`, or a `Window` wrapped in `fromWindow`.

```ts
import { link, worker, connector, handshake, bridge, fromWindow } from '@lickle/wire/browser'

// page — the hub here, workers as its peers
hub.serve(worker('./a.ts'), worker('./b.ts')) // spawned on serve, terminated on teardown
hub.serve(link(existingWorker)) // a target you already hold
hub.serve(persistent(() => link(new Worker('./c.ts')))) // respawns on failure

// worker — the hub here, the page as its peer
hub.serve(link(self))
```

`link` never closes a target it did not create: `own` defaults to true only when the target has
`terminate`, so `link(self)` cannot close the worker it runs in. Ports have no disconnect event, so every
link runs a symmetric heartbeat.

For peers that connect themselves — frames, or anything behind a relay — the handshake transfers one end of
a fresh `MessageChannel` up towards the hub. Ancestors relay only the handshake, never the traffic, because
a transferred port survives being passed onward:

```ts
// the frame
const session = app.connect(connector('app', fromWindow(parent, 'https://host.example')))

// the parent, relaying one hop closer to the worker holding the hub
bridge(window, someWorker, { origins: ['https://frame.example'] })

// the worker
hub.serve(handshake(self, { origins: ['https://frame.example'] }))
```

Origins are required, never `'*'` — defaulting would hand a live `MessagePort` to whoever is listening.

---

## Entry Points

| Import                 | Contents                                                                       |
| ---------------------- | ------------------------------------------------------------------------------ |
| `@lickle/wire`         | `Port`, `Hub`, `Session`, `define`, `pair`, `persistent`, `ILink`, `fakeClock` |
| `@lickle/wire/rpc`     | `pair`, `symmetric`, `send`, `recv`, `both`, `stream`, `mount`, `Frame`        |
| `@lickle/wire/replica` | `define`, `reader`, `writer`, `readerWriter`, `Replica`                        |
| `@lickle/wire/browser` | `link`, `worker`, `connector`, `handshake`, `bridge`, `fromWindow`             |

---

## Testing

`pair()` from the bus is deliberately as harsh as a real transport — structured clone, microtask delivery,
synchronous close — so a test that passes over it passes in production. `fakeClock()` drives backoff,
heartbeats and replication timers deterministically.

```ts
import { Link } from '@lickle/wire'
import { fakeClock } from '@lickle/wire/testing'

const clock = fakeClock()
const link = persistent(() => pair<string>()[0], { clock, jitter: 0, timeout: 100 })
clock.advance(100) // no greeting → retrying
clock.advance(250) // first backoff → second attempt
```

---

## License

MIT © [Dan Beaven](https://github.com/Pingid)
