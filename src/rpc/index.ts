/**
 * Typed request/response and streaming over any {@link Port}.
 *
 * Describe a protocol once with {@link pair}; each side gets a call surface,
 * per-message inbound listeners, and a `serve` for handlers, all derived from
 * the same descriptors so they cannot drift apart. Use {@link symmetric} when
 * both ends run the same code and there is no side to choose.
 */

export type { Port, Unsub } from '../core/index.ts'
export type { Validate } from '../core/validate.ts'
export * from './protocol.ts'
export * from './spec.ts'
