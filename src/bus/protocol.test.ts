import { describe, expect, it } from 'vitest'
import { ProtocolError, VERSION, seal, unseal, type Frame } from './protocol.ts'

const foreign = { ok: false, reason: 'foreign', version: undefined }
const malformed = { ok: false, reason: 'malformed', version: undefined }

describe('seal()', () => {
  it('stamps the hub tag and the wire version onto a copy of the frame', () => {
    const frame: Frame = { t: 'sub', c: 'chat' }
    const e = seal('hub', frame)
    expect(e).toStrictEqual({ t: 'sub', c: 'chat', $: 'hub', v: VERSION })
    expect(e).not.toBe(frame)
    expect(VERSION).toBe(2)
  })
})

describe('unseal()', () => {
  it('treats non-objects as foreign', () => {
    for (const m of [null, undefined, 'ready', 42, true, Symbol('x')]) {
      expect(unseal('hub', m)).toStrictEqual(foreign)
    }
  })

  it("treats another hub's traffic, or an untagged object, as foreign", () => {
    expect(unseal('hub', seal('other', { t: 'sub', c: 'chat' }))).toStrictEqual(foreign)
    expect(unseal('hub', {})).toStrictEqual(foreign)
    expect(unseal('hub', { t: 'sub', c: 'chat', v: VERSION })).toStrictEqual(foreign)
    expect(unseal('hub', [])).toStrictEqual(foreign)
  })

  it('flags an incompatible version and reports which one', () => {
    expect(unseal('hub', { $: 'hub', v: 1, t: 'sub', c: 'chat' })).toStrictEqual({
      ok: false,
      reason: 'version',
      version: 1,
    })
    expect(unseal('hub', { $: 'hub', v: VERSION + 1, t: 'sub', c: 'chat' })).toStrictEqual({
      ok: false,
      reason: 'version',
      version: VERSION + 1,
    })
  })

  it('reports version: undefined when v is missing or not a number', () => {
    expect(unseal('hub', { $: 'hub', v: '2', t: 'sub', c: 'chat' })).toStrictEqual({
      ok: false,
      reason: 'version',
      version: undefined,
    })
    expect(unseal('hub', { $: 'hub', t: 'sub', c: 'chat' })).toStrictEqual({
      ok: false,
      reason: 'version',
      version: undefined,
    })
  })

  it('round-trips every frame kind', () => {
    const frames: Frame[] = [
      { t: 'ready', id: 'h#1' },
      { t: 'sub', c: 'chat' },
      { t: 'unsub', c: 'chat' },
      { t: 'data', c: 'chat', d: { text: 'hi' } },
      { t: 'data', c: 'chat', d: null, to: 'h#2' },
      { t: 'data', c: 'chat', d: [1, 2], from: 'h#3' },
      { t: 'data', c: 'chat', d: 'v', from: 'h#3', r: true },
      { t: 'data', c: 'chat', d: 1, to: 'h#2', from: 'h#3', r: true },
    ]
    for (const f of frames) expect(unseal('hub', seal('hub', f))).toStrictEqual({ ok: true, frame: f })
  })

  it('returns a fresh frame without the envelope fields or unknown extras', () => {
    const sealed = seal('hub', { t: 'sub', c: 'chat' })
    const u = unseal('hub', { ...sealed, extra: 1 })
    expect(u.ok).toBe(true)
    if (!u.ok) return
    expect(u.frame).not.toBe(sealed)
    expect(u.frame).toStrictEqual({ t: 'sub', c: 'chat' })
    expect(u.frame).not.toHaveProperty('$')
    expect(u.frame).not.toHaveProperty('v')
  })

  it('copies to and from only when they are strings, and r only when it is true', () => {
    const base = { $: 'hub', v: VERSION, t: 'data', c: 'chat', d: 1 }
    const bare = { ok: true, frame: { t: 'data', c: 'chat', d: 1 } }
    expect(unseal('hub', { ...base, to: 5, from: null, r: 1 })).toStrictEqual(bare)
    expect(unseal('hub', { ...base, to: undefined, from: {}, r: false })).toStrictEqual(bare)
    expect(unseal('hub', { ...base, r: 'true' })).toStrictEqual(bare)
    expect(unseal('hub', { ...base, to: 'p1', from: 'p2', r: true })).toStrictEqual({
      ok: true,
      frame: { t: 'data', c: 'chat', d: 1, to: 'p1', from: 'p2', r: true },
    })
  })

  it('preserves the payload as-is, including undefined and null', () => {
    const payload = { nested: [1, { a: 'b' }] }
    const u = unseal('hub', { $: 'hub', v: VERSION, t: 'data', c: 'chat', d: payload })
    expect(u.ok).toBe(true)
    if (!u.ok || u.frame.t !== 'data') return
    expect(u.frame.d).toBe(payload)
    expect(unseal('hub', { $: 'hub', v: VERSION, t: 'data', c: 'chat' })).toStrictEqual({
      ok: true,
      frame: { t: 'data', c: 'chat', d: undefined },
    })
    expect(unseal('hub', { $: 'hub', v: VERSION, t: 'data', c: 'chat', d: null })).toStrictEqual({
      ok: true,
      frame: { t: 'data', c: 'chat', d: null },
    })
  })

  it('flags malformed frames', () => {
    const cases: Record<string, unknown>[] = [
      {},
      { t: 'ready' },
      { t: 'ready', id: 7 },
      { t: 'sub' },
      { t: 'sub', c: 1 },
      { t: 'unsub' },
      { t: 'unsub', c: null },
      { t: 'data' },
      { t: 'data', c: 3, d: 1 },
      { t: 'nope', c: 'chat' },
      { t: 42 },
    ]
    for (const c of cases) expect(unseal('hub', { $: 'hub', v: VERSION, ...c })).toStrictEqual(malformed)
  })
})

describe('ProtocolError', () => {
  it('carries fault, detail and a stable name', () => {
    const err = new ProtocolError('payload', 'bad payload on chat', { hub: 'hub', channel: 'chat', peer: 'h#2' })
    expect(err).toBeInstanceOf(Error)
    expect(err).toBeInstanceOf(ProtocolError)
    expect(err.name).toBe('ProtocolError')
    expect(err.message).toBe('bad payload on chat')
    expect(err.fault).toBe('payload')
    expect(err.detail).toEqual({ hub: 'hub', channel: 'chat', peer: 'h#2' })
    expect(String(err)).toBe('ProtocolError: bad payload on chat')
  })

  it('defaults detail to an anonymous hub', () => {
    const err = new ProtocolError('malformed', 'not a frame')
    expect(err.fault).toBe('malformed')
    expect(err.detail).toEqual({ hub: '' })
  })

  it('can carry the offending wire version', () => {
    const err = new ProtocolError('version', 'wire v1 is not supported', { hub: 'hub', version: 1 })
    expect(err.fault).toBe('version')
    expect(err.detail.version).toBe(1)
  })
})
