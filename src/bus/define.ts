import { Hub } from './hub.ts'
import type { Envelope, Topics, Validators } from './protocol.ts'
import { persistent, type Persistent } from './link/index.ts'
import type { ILink } from './link/index.ts'
import { Session } from './session.ts'

export interface DefineOptions<T extends Topics> extends Hub.Options<T> {
  /** Tags every frame, so several buses can share one transport. */
  name: string
  /**
   * Shared by the hub and every session built from this definition, so an
   * inbound payload is narrowed once, at whichever end first sees it.
   */
  validate?: Validators<T> | undefined
}

/** A named bus: the hub and session constructors that share one name and one validation set. */
export interface Definition<T extends Topics> {
  readonly name: string
  /** Router. Call `.serve(source)` to take inbound connections. */
  hub(opts?: Hub.Options<T>): Hub<T>
  /**
   * Client over a link you already hold. No reconnection: when the link dies,
   * the session ends with it. Use this for in-memory pairs and tests.
   */
  session(link: ILink<Envelope>, opts?: Session.Options<T>): Session<T, ILink<Envelope>>
  /**
   * Client that reconnects. Equivalent to
   * `session(Link.persistent(connector, opts), opts)`, but the returned
   * session's `link` is typed as a `Persistent`, so `state`, `attempts`
   * and `retryNow()` are reachable without a cast.
   */
  connect(
    connector: ILink.Connector<Envelope>,
    opts?: Session.Options<T> & Persistent.Options,
  ): Session<T, Persistent<Envelope>>
}

/**
 * Describe a bus once, then mint its hub and sessions by name — for when the
 * two live in different realms and only share this declaration.
 *
 * @example
 * ```ts
 * type Bus = { tick: number }
 * const app = define<Bus>({ name: 'app', retain: ['tick'] })
 * const hub = app.hub()
 * const session = app.connect(connector)
 * ```
 */
export const define = <T extends Topics>(spec: DefineOptions<T>): Definition<T> => {
  const { name, ...shared } = spec
  // `?? shared.validate` rather than spread order: an explicit `validate: undefined`
  // must not erase the definition's validators.
  return {
    name,
    hub: (opts = {}) => Hub.create<T>(name, { ...shared, ...opts, validate: opts.validate ?? shared.validate }),
    session: (link, opts = {}) =>
      Session.over<T, ILink<Envelope>>(name, link, { ...opts, validate: opts.validate ?? shared.validate }),
    connect: (connector, opts = {}) =>
      Session.over<T, Persistent<Envelope>>(name, persistent(connector, opts), {
        ...opts,
        validate: opts.validate ?? shared.validate,
      }),
  }
}
