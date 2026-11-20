/**
 * State-based CRDTs. `merge` must be commutative, associative and idempotent; `value` is the
 * resolver: a pure function of the merged state, so every replica reads the same value.
 */
import { Schema } from "effect"

export interface Crdt<S, V> {
  readonly schema: Schema.Codec<S, unknown>
  readonly empty: S
  readonly merge: (a: S, b: S) => S
  readonly value: (s: S) => V
}

// ---------- slots: one monotonic counter per replica ----------

export type Slots = { readonly [replica: string]: number }
const Slots = Schema.Record(Schema.String, Schema.Number)

const maxSlots = (a: Slots, b: Slots): Slots => {
  const out: Record<string, number> = { ...a }
  for (const [k, v] of Object.entries(b)) out[k] = Math.max(out[k] ?? 0, v)
  return out
}
const total = (s: Slots) => Object.values(s).reduce((x, y) => x + y, 0)

// ---------- PN-counter ----------

export interface PNCounterState {
  readonly p: Slots
  readonly n: Slots
}

export interface PNCounter extends Crdt<PNCounterState, number> {
  readonly add: (s: PNCounterState, replica: string, by: number) => PNCounterState
}

export const PNCounter: PNCounter = {
  schema: Schema.Struct({ p: Slots, n: Slots }),
  empty: { p: {}, n: {} },
  merge: (a, b) => ({ p: maxSlots(a.p, b.p), n: maxSlots(a.n, b.n) }),
  value: (s) => total(s.p) - total(s.n),
  add: (s, replica, by) =>
    by >= 0
      ? { ...s, p: { ...s.p, [replica]: (s.p[replica] ?? 0) + by } }
      : { ...s, n: { ...s.n, [replica]: (s.n[replica] ?? 0) - by } }
}

// ---------- multi-value register ----------

export interface MVEntry<A> {
  readonly value: A
  /** Version vector of the write. */
  readonly clock: Slots
}
export type MVRegisterState<A> = ReadonlyArray<MVEntry<A>>

export interface Resolved<A> {
  readonly value: A | undefined
  /** Concurrent writes not yet superseded, in a replica-independent order. More than one: the resolver chose. */
  readonly candidates: ReadonlyArray<A>
}

export interface MVRegister<A> extends Crdt<MVRegisterState<A>, Resolved<A>> {
  /** A write supersedes every entry this replica has seen. */
  readonly set: (s: MVRegisterState<A>, replica: string, value: A) => MVRegisterState<A>
}

const dominates = (a: Slots, b: Slots) =>
  Object.entries(b).every(([k, v]) => (a[k] ?? 0) >= v) && Object.entries(a).some(([k, v]) => v > (b[k] ?? 0))
const clockKey = (c: Slots) => JSON.stringify(Object.entries(c).sort(([x], [y]) => (x < y ? -1 : 1)))
const byClock = <A>(x: MVEntry<A>, y: MVEntry<A>) => (clockKey(x.clock) < clockKey(y.clock) ? -1 : 1)

/** Keeps concurrent writes; `resolve` picks one on read (e.g. `Math.max`, a priority order, "ask the user"). */
export const MVRegister = <A, E>(schema: Schema.Codec<A, E>, resolve: (concurrent: ReadonlyArray<A>) => A): MVRegister<A> => ({
  schema: Schema.Array(Schema.Struct({ value: schema, clock: Slots })) as Schema.Codec<MVRegisterState<A>, unknown>,
  empty: [],
  merge: (a, b) => {
    const all = [...new Map([...a, ...b].map((e) => [clockKey(e.clock), e])).values()]
    return all.filter((e) => !all.some((o) => dominates(o.clock, e.clock)))
  },
  value: (s) => {
    const candidates = [...s].sort(byClock).map((e) => e.value)
    return { value: candidates.length > 0 ? resolve(candidates) : undefined, candidates }
  },
  set: (s, replica, value) => {
    const seen = s.reduce<Slots>((c, e) => maxSlots(c, e.clock), {})
    return [{ value, clock: { ...seen, [replica]: (seen[replica] ?? 0) + 1 } }]
  }
})
