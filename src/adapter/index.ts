/**
 * Everything for putting `@lickle/wire` on a transport it does not ship with.
 *
 * `@lickle/wire/browser` is written against exactly this and nothing else, so
 * the surface here is not a lesser API for outsiders — it is the one the
 * bundled platform module uses.
 *
 * The layering, innermost first:
 *
 * - a `Port` is two members, and is all `rpc` and `replica` ever wanted;
 * - {@link asLink} lifts one into a `Link`, which is what a `Hub` wants;
 * - {@link defineLink} builds a `Link` straight from a transport, for anything
 *   that can report more than "closed";
 * - {@link keepalive} supplies liveness to transports that cannot;
 * - {@link defineSource} turns a listener into a stream of peers, and
 *   {@link accept} / {@link relay} add the policy that goes with peers who
 *   connect themselves.
 *
 * A platform module's job is only to reach the first of those.
 */

export { asLink, defineLink, defineSource, accept, relay, type Accept, type Link } from '../bus/link/index.ts'
export { keepalive, type Keepalive } from './keepalive.ts'
export { bind, detacher, systemClock, type Clock, type Detacher, type Unsub } from '../core/index.ts'
export { emitter, type Emitter } from '../core/internal.ts'
export type { ListenOptions, Port } from '../core/index.ts'
