import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { Crdt } from "../src/index.js"

const { MVRegister, PNCounter } = Crdt

// Small deterministic PRNG so failures reproduce.
const rng = (seed: number) => () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31)

const randomCounter = (next: () => number): Crdt.PNCounterState => {
  let s = PNCounter.empty
  for (let i = 0; i < 20; i++) s = PNCounter.add(s, `r${Math.floor(next() * 4)}`, Math.floor(next() * 10) - 4)
  return s
}

describe("PNCounter", () => {
  test("merge is commutative, associative and idempotent", () => {
    const next = rng(7)
    for (let i = 0; i < 200; i++) {
      const [a, b, c] = [randomCounter(next), randomCounter(next), randomCounter(next)]
      expect(PNCounter.merge(a, b)).toEqual(PNCounter.merge(b, a))
      expect(PNCounter.merge(PNCounter.merge(a, b), c)).toEqual(PNCounter.merge(a, PNCounter.merge(b, c)))
      expect(PNCounter.merge(a, a)).toEqual(a)
    }
  })

  test("independent increments on separate replicas all count", () => {
    let [x, y, z] = [PNCounter.empty, PNCounter.empty, PNCounter.empty]
    for (let i = 0; i < 100; i++) {
      x = PNCounter.add(x, "x", 1)
      y = PNCounter.add(y, "y", 1)
      z = PNCounter.add(z, "z", -1)
    }
    expect(PNCounter.value([x, y, z].reduce(PNCounter.merge))).toBe(100)
  })
})

describe("MVRegister", () => {
  const Setpoint = MVRegister(Schema.Number, (concurrent) => Math.max(...concurrent))

  test("concurrent writes are kept and resolved the same way in any merge order", () => {
    const a = Setpoint.set(Setpoint.empty, "x", 19)
    const b = Setpoint.set(Setpoint.empty, "y", 22)
    const c = Setpoint.set(Setpoint.empty, "z", 21)
    const left = Setpoint.value(Setpoint.merge(Setpoint.merge(a, b), c))
    const right = Setpoint.value(Setpoint.merge(c, Setpoint.merge(b, a)))
    expect(left).toEqual(right)
    expect(left.value).toBe(22)
    expect([...left.candidates].sort()).toEqual([19, 21, 22])
  })

  test("a write after seeing every candidate supersedes them", () => {
    const merged = [
      Setpoint.set(Setpoint.empty, "x", 19),
      Setpoint.set(Setpoint.empty, "y", 22)
    ].reduce(Setpoint.merge)
    const later = Setpoint.set(merged, "x", 20)
    expect(Setpoint.value(Setpoint.merge(merged, later))).toEqual({ value: 20, candidates: [20] })
  })
})
