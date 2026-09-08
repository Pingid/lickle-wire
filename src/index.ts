/**
 * Composable transport, pub/sub, RPC and state-replication primitives with
 * zero runtime dependencies.
 *
 * Everything speaks one contract, {@link Port}: a link is a port, a topic is a
 * port, and `@lickle/wire/rpc` and `@lickle/wire/replica` run over any port.
 */

export type { Clock, ListenOptions, Port, Unsub, Validate } from './core/index.ts'
export { queue, type Queue, type QueueIterator, type QueueOptions } from './core/index.ts'

export type { Replica } from './replica/index.ts'
export * as replica from './replica/index.ts'
export * as rpc from './rpc/index.ts'

export * from './bus/index.ts'
export { fakeClock, type FakeClock } from './bus/testing.ts'
