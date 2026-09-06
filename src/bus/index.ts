// The framing itself — `seal`/`unseal`/`Frame`/`VERSION` — stays internal: it is
// the hub's business, not a consumer's. What is public is what a consumer holds
// or handles: the wire type, the validators they supply, and the error they catch.
export type { Envelope, ProtocolFault, Topics, Validators } from './protocol.ts'
export { ProtocolError } from './protocol.ts'

export { Session } from './session.ts'
export { Hub } from './hub.ts'
export * from './link/index.ts'

export * from './define.ts'
