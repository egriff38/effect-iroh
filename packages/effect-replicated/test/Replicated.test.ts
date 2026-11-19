import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer, Schedule, Schema, SubscriptionRef } from "effect"
import { RpcTest } from "effect/rpc"
import { Crdt, Replica, Replicated } from "../src/index.js"

const Setpoint = Crdt.MVRegister(Schema.Number, (concurrent) => Math.max(...concurrent))

class Thermostat extends Context.Service<Thermostat, {
  readonly energy: Replica.Field<Crdt.PNCounterState, number>
  readonly setpoint: Replica.Field<Crdt.MVRegisterState<number>, Crdt.Resolved<number>>
}>()("test/Thermostat") {}

const ThermostatLive = Replicated.layer(Thermostat, { energy: Crdt.PNCounter, setpoint: Setpoint })

/**
 * Three replicas in one process, wired with RpcTest: each replica's `client(peer)` runs the
 * peer's real ReplicaSync handlers in memory. No network, same code path as over iroh.
 */
const cluster = Effect.gen(function*() {
  const ids = ["x", "y", "z"]
  // Every Replica exists before any service starts pulling from its peers.
  const replicas = new Map<string, Replica.Replica["Service"]>()
  for (const self of ids) {
    const ctx = yield* Layer.build(Replica.layer({
      self,
      peers: () => ids.filter((id) => id !== self),
      client: (peer) =>
        RpcTest.makeClient(Replica.ReplicaSync).pipe(
          Effect.provide(Replicated.layerHandlers.pipe(
            Layer.provide(Layer.succeed(Replica.Replica)(replicas.get(peer)!))
          ))
        )
    }))
    replicas.set(self, Context.get(ctx, Replica.Replica))
  }
  return yield* Effect.forEach(ids, (self) =>
    Effect.map(
      Layer.build(ThermostatLive.pipe(Layer.provide(Layer.succeed(Replica.Replica)(replicas.get(self)!)))),
      (ctx) => Context.get(ctx, Thermostat)
    ))
})

/** Re-reads until `done` holds (or 3 s pass), so the test waits for convergence, not a guess. */
const until = <A>(read: Effect.Effect<A>, done: (a: A) => boolean) =>
  read.pipe(
    Effect.repeat({ until: done, schedule: Schedule.spaced("20 millis") }),
    Effect.timeoutOption("3 seconds"),
    Effect.andThen(read)
  )

describe("Replicated", () => {
  test("concurrent increments on every replica converge to the exact total", () =>
    Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const nodes = yield* cluster
      yield* Effect.forEach(nodes, (node) =>
        Effect.forEach(Array.from({ length: 50 }), () =>
          node.energy.update((s, me) => Crdt.PNCounter.add(s, me, 1))), { concurrency: "unbounded" })
      const totals = yield* until(
        Effect.forEach(nodes, (n) => Effect.map(SubscriptionRef.get(n.energy.state), Crdt.PNCounter.value)),
        (ts) => ts.every((t) => t === 150)
      )
      expect(totals).toEqual([150, 150, 150])
    }))))

  test("conflicting writes resolve identically everywhere; a later write supersedes them", () =>
    Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const nodes = yield* cluster
      const [x] = nodes
      const read = Effect.forEach(nodes, (n) => Effect.map(SubscriptionRef.get(n.setpoint.state), Setpoint.value))

      yield* Effect.forEach(nodes, (n, i) => n.setpoint.update((s, me) => Setpoint.set(s, me, [19, 22, 21][i]!)))
      const concurrent = yield* until(read, (rs) => rs.every((r) => r.candidates.length === 3))
      expect(concurrent.map((r) => r.value)).toEqual([22, 22, 22])
      expect(new Set(concurrent.map((r) => r.candidates.join()))).toHaveLength(1)

      yield* x!.setpoint.update((s, me) => Setpoint.set(s, me, 20))
      expect(yield* until(read, (rs) => rs.every((r) => r.candidates.length === 1 && r.value === 20)))
        .toEqual([0, 1, 2].map(() => ({ value: 20, candidates: [20] })))
    }))))
})
