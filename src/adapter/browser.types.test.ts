/**
 * Assignability assertions for the real DOM types. Checked by `tsc`, not
 * collected by vitest: every positive is a shape the adapter promises to
 * accept, every negative one it promises to reject — and a negative that stops
 * being an error fails the build as an unused directive, so this guards both
 * directions.
 */
import { test } from 'vitest'
import type { Port } from '../core/index.ts'
import { asLink, type Link, type Transfer } from '../bus/link/index.ts'
import {
  asDuplex,
  asPort,
  asTarget,
  asTransferPort,
  bridge,
  connector,
  fromServiceWorker,
  fromWindow,
  handshake,
  link,
  workerSelf,
  type Browser,
} from './browser.ts'

test('types', () => {})

declare const w: Worker
declare const mp: MessagePort
declare const bc: BroadcastChannel
declare const win: Window
declare const par: WindowProxy
declare const gt: typeof globalThis
declare const sw: ServiceWorker
declare const swc: ServiceWorkerContainer
declare const ws: WebSocket
declare const plain: Port<number, number>
/** A channel that is not a `MessagePort`: an extension runtime port. */
interface RuntimePort {
  readonly name: string
}
declare const extension: Transfer.Offers<RuntimePort>

/**
 * Never called. The fixtures above are `declare const`s with no runtime value,
 * so these are assertions for `tsc` and must not execute.
 */
const conformance = () => {
  /* -------------------------------------------------------------------------- */
  /* Duplex: one object that both posts and receives                            */
  /* -------------------------------------------------------------------------- */

  const d1: Browser.Duplex = w
  const d2: Browser.Duplex = mp
  const d3: Browser.Duplex = bc
  const d4: Browser.Duplex = sw
  void [d1, d2, d3, d4]

  // @ts-expect-error a Window's postMessage demands a targetOrigin — wrap it in fromWindow
  const d5: Browser.Duplex = win
  // @ts-expect-error `self` is typed as a Window under lib:["DOM"] — use workerSelf()
  const d6: Browser.Duplex = gt
  // @ts-expect-error you post to a ServiceWorker, and hear on the container
  const d7: Browser.Duplex = swc
  // @ts-expect-error a socket sends; it does not postMessage
  const d8: Browser.Duplex = ws
  void [d5, d6, d7, d8]

  /* -------------------------------------------------------------------------- */
  /* Events: the receive half takes everything                                  */
  /* -------------------------------------------------------------------------- */

  const e1: Browser.MessageEventTarget = w
  const e2: Browser.MessageEventTarget = mp
  const e3: Browser.MessageEventTarget = bc
  const e4: Browser.MessageEventTarget = sw
  const e5: Browser.MessageEventTarget = swc
  const e6: Browser.MessageEventTarget = win
  const e7: Browser.MessageEventTarget = gt
  const e8: Browser.MessageEventTarget = ws
  void [e1, e2, e3, e4, e5, e6, e7, e8]

  /* -------------------------------------------------------------------------- */
  /* Post: the send half                                                        */
  /* -------------------------------------------------------------------------- */

  const p1: Browser.Post = w
  const p2: Browser.Post = mp
  const p3: Browser.Post = bc
  const p4: Browser.Post = sw
  void [p1, p2, p3, p4]

  // @ts-expect-error a Window's postMessage demands a targetOrigin
  const p5: Browser.Post = win
  // @ts-expect-error the container is receive-only
  const p6: Browser.Post = swc
  void [p5, p6]

  /* -------------------------------------------------------------------------- */
  /* the public API accepts each of them                                        */
  /* -------------------------------------------------------------------------- */

  void link(w)
  void link(mp)
  void link(bc)
  void link(sw)
  void link(workerSelf())
  void link(fromWindow(par, 'https://host.example'))
  void link(fromServiceWorker(() => navigator.serviceWorker.controller))

  // A socket is a `Split` a consumer writes inline: nothing about it needs to
  // live in the package.
  void link({
    to: { postMessage: (m: unknown) => ws.send(JSON.stringify(m)), close: () => ws.close() },
    from: (fn) => {
      const h = (e: MessageEvent) => fn(new MessageEvent('message', { data: JSON.parse(String(e.data)) }))
      ws.addEventListener('message', h)
      return () => ws.removeEventListener('message', h)
    },
  })

  // @ts-expect-error a Window is not a target; wrap it in fromWindow
  void link(win)
  // @ts-expect-error `self` in a worker is not a target; use workerSelf()
  void link(gt)

  // A receive-only site takes anything inbound, a bare Window included.
  void handshake(gt)
  void handshake(win)
  void handshake(swc)
  void handshake(navigator.serviceWorker)
  void handshake(fromWindow(par, 'https://host.example').from)

  // A send-only site takes anything outbound, a split included.
  void connector('app', w)
  void connector('app', sw)
  void connector('app', fromWindow(par, 'https://host.example'))
  void bridge(gt, w)
  void bridge(win, fromWindow(par, 'https://host.example'))

  // @ts-expect-error a WebSocket cannot be posted to
  void connector('app', ws)

  /* -------------------------------------------------------------------------- */
  /* the layering: a platform module stops at Port, everything above lifts      */
  /* -------------------------------------------------------------------------- */

  // Every Duplex is an on-ramp to the two-member contract...
  const q1: Port<number, number> = asPort<number>(w)
  const q2: Port<number, number> = asPort<number>(mp)
  const q3: Port<number, number> = asPort<number>(bc)
  const q4: Port<number, number> = asPort<number>(fromWindow(par, 'https://host.example'))
  void [q1, q2, q3, q4]

  // ...and `asLink` is the way up a tier, from the library, not from here.
  const lifted: Link<number> = asLink(asPort<number>(w))
  void lifted

  // `link` is the both-at-once convenience, and is still a Port.
  const linkIsAPort: Port<number, number> = link<number>(w)
  void linkIsAPort

  // @ts-expect-error a Port is not a Link: liveness has to be lifted in
  const notALink: Link<number> = asPort<number>(w)
  void notALink

  // Both faces at once, each the real type.
  const bothLink: Link<number> = asDuplex(asLink(asPort<number>(w)))
  const bothTarget: Browser.Duplex = asDuplex(asLink(asPort<number>(w)))
  void [bothLink, bothTarget]
  void link(asDuplex(asLink(asPort<number>(w))))

  // @ts-expect-error it takes a Link; a bare Port has no liveness to keep
  void asDuplex(asPort<number>(w))

  // The inverse: any Port becomes a target, so the two compose either way.
  const back: Browser.Duplex = asTarget(asPort<number>(w))
  void back
  void asPort(asTarget(asPort<number>(w)))
  void link(asTarget(asPort<number>(w)))
  // A Link keeps its lifecycle when used as a target, because `close` forwards.
  void asTarget(link<number>(w))

  // @ts-expect-error a target is not a Port; convert it
  void asLink<number>(w)

  /* -------------------------------------------------------------------------- */
  /* moving a connection is a capability, not a second argument                 */
  /* -------------------------------------------------------------------------- */

  // Every duplex target yields one...
  const t1: Transfer.Offers<MessagePort> = asTransferPort(w)
  const t2: Transfer.Offers<MessagePort> = asTransferPort(mp)
  const t3: Transfer.Offers<MessagePort> = asTransferPort(fromWindow(par, 'https://host.example'))
  void [t1, t2, t3]

  // ...and the handshake takes either that or the DOM shape it came from.
  void handshake(t1)
  void handshake(gt)
  void connector('app', t1)
  void connector('app', w)
  void bridge(gt, t1)
  void bridge(t1, w)

  // A `Port` moves values, not ownership, so it cannot carry a handshake.
  // @ts-expect-error a plain Port is not a transfer port
  const notOffers: Transfer.Offers<MessagePort> = plain
  void notOffers
  // @ts-expect-error ...and a Link is not one either
  void connector('app', asLink(asPort<number>(w)))

  // The channel type is generic, so a platform with its own is the same shape.
  const alsoOffers: Transfer.Offers<RuntimePort> = extension
  void alsoOffers

  /* -------------------------------------------------------------------------- */
  /* a hand-rolled stub                                                         */
  /* -------------------------------------------------------------------------- */

  const stub: Browser.Duplex = {
    postMessage(_m: unknown) {},
    addEventListener(_t: 'message', _f: (e: MessageEvent) => void) {},
    removeEventListener(_t: 'message', _f: (e: MessageEvent) => void) {},
  }
  void link(stub)
}
void conformance
