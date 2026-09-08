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

const session = Session.over<Channels>('app', link(workerSelf()))
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

A link is a port. A topic is a port. Either side of an RPC protocol is a port. `asPort(worker)` is a
port. So RPC and replication run over any of them — a worker directly, a hub topic, or a test double —
without adapters. Anything you build that satisfies the interface plugs in the same way.

`Port` is the currency, and there is exactly one tier above it. `rpc` and `replica` take a `Port`
because two members is all they ever needed. `Hub`, `Session` and `persistent` take a `Link`, because
routing needs to know when a peer went away. Going down is free — a `Link` _is_ a `Port` — and `asLink`
is the way back up:

```ts
import { asLink } from '@lickle/wire'

hub.serve(asLink(anyPort)) // `up` is "not closed"; the port's `onClose` becomes `closed`
```

A platform module's whole job is to get you to `Port`. Everything above it is generic.

### Link

Layer 0: one end of an open duplex channel, with reconnection.

```ts
import { pair, persistent } from '@lickle/wire'

const [a, b] = pair<string>() // two cross-wired in-memory ends
b.listen((m) => console.log(m))
a.send('hi') // structured-cloned and delivered on a microtask, like a real transport

const link = persistent(connector, {
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

A `Link` adds `remote`, `meta`, `up`, `changed(fn)`, `closed(fn)` and `close()` to the port contract. `send`
never throws and returns `false` when a message was dropped. Inbound messages that arrive before the first
`listen` are buffered, so a greeting is never lost.

`up` and "open" are separate: a link can be alive and down — `persistent` between attempts, a socket
still connecting — which is why `changed` exists alongside `closed`. `closed` is terminal.

A `slot` is a link whose transport you swap by hand. Consumers hold the slot and subscribe once; every
`listen`, `changed` and `closed` survives the swap, and only the slot's own subscription to the inner
link is torn down and rebuilt:

```ts
import { slot } from '@lickle/wire'

const wire = slot<Envelope>({ buffer: 64 })
const session = app.session(wire) // subscribes once, to the slot

const off = wire.use(link(new Worker('./a.ts'))) // attach; `off` detaches
wire.use(link(new Worker('./b.ts'))) // swap — `off` is now inert
wire.inner // the transport in use, or null
```

`use` returns a teardown like every other subscription here, and the teardown of a transport that has
since been replaced does nothing — so detaching late cannot cut someone else's wire. An inner closing
detaches the slot rather than ending it; only `slot.close()` is terminal. Sends made while nothing is
attached are held per `buffer`/`ttl`/`overflow` and flushed when one is.

`persistent` is a slot with a retry policy driving the swaps. Reach for a slot when the schedule is not
a backoff curve: a peer the user picks from a list, a worker swapped on hot reload, a test that wants to
cut the wire and splice it back.

### Session and Topic

Layer 2: the client side. A session owns its _subscription intent_ — the link is disposable, the intent is
not, and it is replayed on every reconnect.

```ts
const session = Session.over<Channels>('app', link(workerSelf())) // ends when the link does
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

const rr = session.topic('req', 'res') // publish on one channel, listen on another
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
    canPublish: (peer, channel, payload) => true,
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

### Meta

Every frame has room for a bag of arbitrary keys beside the payload — a trace id, a token, a clock
reading. It sits outside the spec and outside validation: a descriptor says what a message _means_, and
meta says something about the circumstances it was sent in.

```ts
l.log('hello', { meta: { trace } }) // notifications
await l.getUser({ id: '1' }, { meta: { trace }, signal }) // calls
for await (const row of l.watch(query, { meta: { trace } })) {
} // streams

r.serve({
  log: (s, ctx) => console.log(s, ctx.meta['trace']),
  getUser: ({ id }, ctx) => lookup(id, ctx.meta['token']),
})

r.on.log((s, meta) => console.log(s, meta['trace'])) // and under `on`, as a second argument
```

Responses carry it too — `respond(value, meta)`, `next(value, meta)`, `end(meta)`, `fail(error, meta)`.
A call resolves to its payload and nothing else, so response meta is read off the side itself, which is a
port over decoded messages:

```ts
l.listen((m) => {
  if (m.re === 'ok') console.log(m.meta)
})
```

A codec can contribute meta too, which is how a layer passes down what it alone knows — the hop it came
over, the identity it just verified. Whatever it hands `next` as a second argument joins the frame's own:

```ts
const enveloped = api.with<Env>({
  encode: (frame) => ({ hop: here, frame }),
  decode: (env, next) => next(env.frame, { hop: env.hop }),
})
```

Layers merge outermost-last, and every layer outranks the wire, so what a codec vouches for cannot be
spoofed by the far side claiming the same key.

A frame that carries no meta has no `meta` key, and a listener that was sent none is handed `{}`. The
contents are never validated; the shape is — a frame whose `meta` is not an object is rejected like any
other malformed frame.

One cost: a lone argument is ambiguous between a payload and options, so options are recognised
structurally — a live `AbortSignal` in `signal`, or an object carrying nothing but `meta`. A payload that
is itself exactly `{ meta }` is therefore read as options; pass the options explicitly to disambiguate,
`l.echo({ meta: x }, {})`. The same already applied to `{ signal }`.

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
{ _t: 'api', kind: 'log', payload: 'hi', meta: { trace: 't' } }   // any frame, when meta was attached
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

The DOM has no single "thing you post messages to", so `Browser.Target` is a union of the two shapes it
really has. A `Worker`, a `MessagePort`, a `BroadcastChannel` and a `ServiceWorker` are `Browser.Duplex` —
one object that both posts and receives — and go in as they are. Everything else is a `Browser.Split`: a
send half and a receive half that are different objects, which is what `fromWindow`, `fromServiceWorker`
and `workerSelf` build. A `WebSocket` is a `Split` you write inline in six lines; it needs nothing from
this package.

The module itself is thin. It contributes those types and two conversions; everything else is
composition over the library's own generics.

```ts
import { asPort, asTarget } from '@lickle/wire/browser'

// DOM -> wire. The whole on-ramp: two members, no lifecycle, no heartbeat.
rpc.left(asPort(worker))
replica.reader(def, asPort(broadcastChannel))
hub.serve(asLink(asPort(worker))) // ...and up a tier when routing needs liveness

// wire -> DOM. A topic, an rpc side or a link, for code that speaks postMessage.
comlinkish(asTarget(session.topic('api')))
```

`asPort` attaches on the first listener and detaches on the last, like every `Port`, and its `onClose`
never fires — a `MessagePort` has no disconnect event, which is exactly what `link`'s heartbeat covers.
`asTarget` goes the other way and `close` forwards to the source's own, so a `Link` used as a target
keeps its lifecycle. The two compose in either order, which is what makes adapters stackable.

`asTarget` converts and forgets: what comes back is a `Browser.Duplex` and nothing else. When the caller
still needs `up`, `changed` or `closed`, `asDuplex` keeps both faces on one object instead:

```ts
const both = asDuplex(muxChannel) // Link<T> & Browser.Duplex

thirdParty(both) // speaks postMessage
both.changed((up) => render(up)) // ...and still a link
both.close() // one connection, closed once, whichever face you reach for
```

`remote`, `meta` and `up` stay live rather than snapshotted, so a `slot` or a `persistent` underneath
still tracks. There is no `start` or `terminate` — a link is already running, and is not a resource
this can destroy.

`link` is the both-at-once convenience — `asPort`'s normalisation, plus liveness, plus ownership:

```ts
import {
  link,
  worker,
  connector,
  handshake,
  bridge,
  fromWindow,
  fromServiceWorker,
  workerSelf,
} from '@lickle/wire/browser'

// page — the hub here, workers as its peers
hub.serve(worker('./a.ts'), worker('./b.ts')) // spawned on serve, terminated on teardown
hub.serve(link(existingWorker)) // a target you already hold
hub.serve(persistent(() => link(new Worker('./c.ts')))) // respawns on failure

// worker — the hub here, the page as its peer
hub.serve(link(workerSelf()))

// a service worker: you post to one object and hear on another
hub.serve(link(fromServiceWorker(() => navigator.serviceWorker.controller)))
```

`link` never closes a target it did not create: `own` defaults to true only when the target has
`terminate`, so `link(workerSelf())` cannot close the worker it runs in. Ports have no disconnect event, so every
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
hub.serve(handshake(workerSelf(), { origins: ['https://frame.example'] }))
```

Origins are required, never `'*'` — defaulting would hand a live `MessagePort` to whoever is listening.
`handshake` returns a source that owns what it produced: its teardown disconnects the peers it accepted,
it does not merely stop accepting new ones.

`handshake` and `bridge` are `accept` and `relay` with `MessageChannel` filled in. The policy — which
offers to admit, in what order the checks run, how a relayed hop is recorded, and the rule that
declining must never touch the channel — lives in the library, so an extension or a server gets the
same behaviour without reimplementing it.

None of the three take a `Link`, and cannot: they move a `MessagePort`, and `Link.send` has no transfer
list. That is the mechanism, not a gap in the types — transferring is exactly what keeps an ancestor
out of the data path. When the next hop _cannot_ take a port, use `tunnel` and a [`mux`](#mux):

```ts
// the parent, whose next hop is a server rather than a Window
const up = mux<Envelope>(socketLink, { side: 'a' })
tunnel(window, up, { origins: ['https://frame.example'] })
```

`bridge` hands the port on and steps out of the way; `tunnel` carries the traffic. Both vet offers
identically. Prefer `bridge` whenever the next hop can take a port.

#### Relaying to a worker you replace

A `Browser.Sink` is just an object with `postMessage`, so make it indirect and the relay is wired once
and always aims at the current worker — no need to re-run `bridge` on every swap:

```ts
const wire = slot<Envelope>({ own: true }) // the page's own peer link
let current: Worker | null = null

const to: Browser.Sink = { postMessage: (m, t) => current?.postMessage(m, t ?? []) }
bridge(window, to, { origins: ['https://frame.example'] }) // once, for good

const spawn = () => {
  current = new Worker('./hub.js', { type: 'module' })
  wire.use(link(current)) // closes the worker it replaces, under `own`
}
```

Ports are transferred straight to whichever worker is current, so the page never joins the data path.
Frames connected to the worker that went away reconnect on their own, because `connector` mints a
fresh channel per attempt.

Use `tunnel` and a [`mux`](#mux) over the slot instead when frames must survive the swap _without_
reconnecting, or when the next hop cannot take a port at all. A mux re-announces its channels on the
new transport, so a channel opened over the old one keeps working.

### Transfer

`Port.send` takes one argument and always has. A transfer list as a second parameter would put "can
this transport hand over ownership?" into every signature that mentions a port, for the one family of
transports that can — so it rides in the message instead, and the capability becomes a type:

```ts
interface Transfer.Out<S, C> { data: S; transfer?: readonly C[] }              // what you hand over
interface Transfer.In<R, C>  { data: R; transfer: readonly C[]; origin: string } // what you were handed

interface TransferPort<S, R = S, C = unknown> extends Port<Transfer.Out<S, C>, Transfer.In<R, C>> {}
```

Two frames, not one shared shape: outbound you say what you are giving away, inbound you are told what
you were given _and where it came from_. `origin` belongs only on the second, and is per-message rather
than per-connection — a page with three frames posts to one `window`, so it cannot live on `meta`.

`C` is whatever the platform calls a connection: a `MessagePort` in a page, a `runtime.Port` in an
extension, a socket on a server. That is what makes the handshake generic — `postOffer`, `readOffers`
and `passOffer` are the three roles, and they feed `accept`/`relay` directly:

```ts
import { postOffer, readOffers, passOffer, accept, relay } from '@lickle/wire'

postOffer(up, 'panel', channel) // connector
accept(readOffers(up), wrap, { origins }) // acceptor
relay(readOffers(from), passOffer(up), { origins }) // relay
```

`@lickle/wire/browser` supplies only `transferPort(target)`, which unwraps `postMessage(data, transfer)`
and packs `event.ports`/`event.origin`. A plain `Port` or `Link` is not a `TransferPort`, so handing one
to `connector` is a type error rather than a handshake that half-completes.

### Mux

Many logical links over one real one, for when peers have to connect through a connection you already
hold. Platform-free — it works over any `Link`, so an extension, a socket or a worker all get it.

```ts
import { mux } from '@lickle/wire'

const wire = mux<Envelope>(socketLink, { side: 'a' }) // the other end is 'b'

const session = app.session(wire.open('panel')) // a channel, which is just a Link
hub.serve(wire.incoming) // channels the far side opened, as peers
```

Each end allocates ids from its own half of the number space — one odd, one even — so the two can open
at the same moment without negotiating. An `open` arriving from this end's half means both were
configured as the same `side`, and is reported rather than silently colliding. Channels that arrive
before anything serves `incoming` are held, for the same reason a link buffers its greeting.

A mux is honest about its cost: unlike a transferred port, it carries every channel's traffic itself.
It also does not repeat buffering — put a `persistent` or a `slot` underneath and one outbox covers
every channel at once. `pipe(a, b)` joins two links when a relay has to sit in the middle, which is
what `tunnel` uses.

---

## Writing an Adapter

A transport is `defineLink`: `open` is handed the inbound half and returns the outbound half. The link
core does the rest — hold inbound until someone listens, track `up` transitions, fire `closed` exactly
once, go inert afterwards rather than throw.

```ts
import { defineLink, defineSource, keepalive, type Link } from '@lickle/wire/adapter'
```

| You have                                 | You write                           |
| ---------------------------------------- | ----------------------------------- |
| A real disconnect event                  | `defineLink` alone                  |
| No disconnect event                      | `defineLink` + `keepalive`          |
| Something that connects, not connects to | `defineLink` + `defineSource`       |
| Something that reconnects                | wrap your connector in `persistent` |

### A `chrome.runtime.Port`

No heartbeat: `onDisconnect` fires on both sides whenever the other context goes, including when an MV3
service worker is torn down, so liveness is a fact rather than something to infer. Probing it would add
traffic and a second way to be wrong — and an adapter that never imports `keepalive` carries no timer
code at all.

```ts
const chromeLink = <T>(
  port: chrome.runtime.Port,
  runtime: typeof chrome.runtime,
  opts: { meta?: Link.Meta } = {},
): Link<T> =>
  defineLink<T>(
    (host) => {
      const onMessage = (msg: unknown) => host.deliver(msg as T)
      // Reading `lastError` is both how chrome tells you why the port died and
      // how you stop it logging an unchecked-error warning.
      const onDisconnect = () => {
        void runtime.lastError
        host.shut()
      }
      port.onMessage.addListener(onMessage)
      port.onDisconnect.addListener(onDisconnect)

      return {
        send: (msg) => {
          try {
            port.postMessage(msg)
            return true
          } catch (err) {
            // The far end went away between the check and the post.
            host.fail(err)
            host.shut()
            return false
          }
        },
        release: () => {
          port.onMessage.removeListener(onMessage)
          port.onDisconnect.removeListener(onDisconnect)
          port.disconnect()
        },
      }
    },
    { remote: port.name, meta: { ...opts.meta, sender: port.sender } },
  )

// Every context that connects, as a peer: `hub.serve(chromeSource(chrome.runtime))`.
const chromeSource = <T>(runtime: typeof chrome.runtime): Link.Source<T> =>
  defineSource<T>((host) => {
    const onConnect = (port: chrome.runtime.Port) => host.offer(chromeLink<T>(port, runtime))
    runtime.onConnect.addListener(onConnect)
    return () => runtime.onConnect.removeListener(onConnect)
  })
```

`defineSource` owns what it produced: the teardown closes links still open and forgets ones that closed
themselves, so stopping a source disconnects its peers instead of leaking them.

When peers connect themselves and have to be vetted, `accept` is `defineSource` plus the policy — an
origin allowlist, a `filter` that is shown the offer but never the channel, and the hop recording that
builds `peer.meta.path`. The platform supplies only how an offer arrives:

```ts
const chromeAccept = <T>(runtime: typeof chrome.runtime): Link.Source<T> =>
  accept<T, chrome.runtime.Port>(
    (take) => {
      const on = (p: chrome.runtime.Port) =>
        take({ name: p.name, origin: p.sender?.origin ?? '', path: [], channel: p })
      runtime.onConnect.addListener(on)
      return () => runtime.onConnect.removeListener(on)
    },
    (offer, meta) => chromeLink<T>(offer.channel, runtime, { meta }),
    { origins: [`chrome-extension://${runtime.id}`] },
  )
```

### A node `worker_threads` port

`@types/node` models it as an `EventEmitter`, so it satisfies no DOM target type — which costs nothing,
because `defineLink` never asked for one.

```ts
const threadLink = <T>(port: MessagePort): Link<T> =>
  defineLink<T>((host) => {
    const on = (msg: T) => host.deliver(msg)
    port.on('message', on)
    port.once('close', host.shut)
    return {
      send: (msg) => (port.postMessage(msg), true),
      release: () => {
        port.off('message', on)
        port.close()
      },
    }
  })
```

### A `WebSocket`

The case where `up` is not "not shut": a socket is alive and unusable while it is connecting, and
`readyState` is the truth. `host.alive` is whether the link is over; `up` is whether `send` can reach
anyone. Wrap it in `persistent` and sends made while down are held rather than dropped.

```ts
const socketLink = <T>(url: string): Link<T> =>
  defineLink<T>(
    (host) => {
      const ws = new WebSocket(url)
      const onOpen = () => host.signal(true)
      const onMessage = (e: MessageEvent) => {
        try {
          host.deliver(JSON.parse(String(e.data)) as T)
        } catch (err) {
          // A frame we cannot read is a bad payload, not a dead socket.
          host.fail(err)
        }
      }
      // `error` never arrives without a `close` behind it, so only `close` is terminal.
      const onError = (e: Event) => host.fail(e)
      const onClose = () => host.shut()

      ws.addEventListener('open', onOpen)
      ws.addEventListener('message', onMessage)
      ws.addEventListener('error', onError)
      ws.addEventListener('close', onClose)

      return {
        send: (msg) => {
          if (ws.readyState !== WebSocket.OPEN) return false
          ws.send(JSON.stringify(msg))
          return true
        },
        release: () => {
          ws.removeEventListener('open', onOpen)
          ws.removeEventListener('message', onMessage)
          ws.removeEventListener('error', onError)
          ws.removeEventListener('close', onClose)
          if (ws.readyState < WebSocket.CLOSING) ws.close(1000)
        },
      }
    },
    { remote: url, meta: { url }, up: false },
  )
```

### The pieces

| Export                                      | For                                                              |
| ------------------------------------------- | ---------------------------------------------------------------- |
| `host.deliver` / `signal` / `shut` / `fail` | everything a transport can report; `alive` is whether it is over |
| `keepalive(send, onDead, opts)`             | ping/pong for transports with no disconnect event                |
| `defineSource(open, opts)`                  | a listener that yields peers, owning what it produced            |
| `detacher()`                                | a teardown set that cannot be beaten by a synchronous `closed`   |
| `bind(attach, signal)`                      | attach and detach as one expression                              |
| `emitter()`                                 | signal-aware listener bookkeeping                                |
| `outbox(opts)`                              | holding sends while a link is down, with ttl and overflow        |

---

## Entry Points

| Import                 | Contents                                                                                                                                                          |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@lickle/wire`         | `Port`, `Link`, `Hub`, `Session`, `define`, `pair`, `persistent`, `asLink`, `queue`, `fakeClock`                                                                  |
| `@lickle/wire/rpc`     | `pair`, `symmetric`, `send`, `recv`, `both`, `stream`, `mount`, `Frame`                                                                                           |
| `@lickle/wire/replica` | `define`, `reader`, `writer`, `readerWriter`, `Replica`                                                                                                           |
| `@lickle/wire/adapter` | `defineLink`, `defineSource`, `keepalive`, `detacher`, `bind`, `emitter`                                                                                          |
| `@lickle/wire/browser` | `asPort`, `asTarget`, `asDuplex`, `transferPort`, `link`, `worker`, `connector`, `handshake`, `bridge`, `tunnel`, `fromWindow`, `fromServiceWorker`, `workerSelf` |
| `@lickle/wire/testing` | `fakeClock`                                                                                                                                                       |

---

## Testing

`pair()` from the bus is deliberately as harsh as a real transport — structured clone, microtask delivery,
synchronous close — so a test that passes over it passes in production. `fakeClock()` drives backoff,
heartbeats and replication timers deterministically.

```ts
import { pair, persistent } from '@lickle/wire'
import { fakeClock } from '@lickle/wire/testing'

const clock = fakeClock()
const link = persistent(() => pair<string>()[0], { clock, jitter: 0, timeout: 100 })
clock.advance(100) // no greeting → retrying
clock.advance(250) // first backoff → second attempt
```

---

## License

MIT © [Dan Beaven](https://github.com/Pingid)
