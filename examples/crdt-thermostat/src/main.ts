/**
 * Three CRDT replicas over a private iroh network, in one process. The element module at the top
 * is plain Effect + atoms; the wiring below picks iroh as the transport.
 */
import { Context, Effect, Layer, Schema } from "effect"
import { NetAddress } from "effect/net"
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { IrohEndpoint, IrohRpc } from "effect-iroh"
import { Crdt, Replica, Replicated } from "effect-replicated"

// ── element module: a service of CRDT fields, plain atoms over it ────────────

class Thermostat extends Context.Service<Thermostat, {
  readonly energy: Replica.Field<Crdt.PNCounterState, number>
  readonly setpoint: Replica.Field<Crdt.MVRegisterState<number>, Crdt.Resolved<number>>
}>()("example/Thermostat") {}

// The resolver is domain policy: concurrent setpoints resolve to the highest.
const Setpoint = Crdt.MVRegister(Schema.Number, (concurrent) => Math.max(...concurrent))
const ThermostatLive = Replicated.layer(Thermostat, { energy: Crdt.PNCounter, setpoint: Setpoint })

// Default context: offline. Each node swaps in its networked Replica.
const runtime = Atom.runtime(ThermostatLive.pipe(Layer.provide(Replica.layerOffline)))

const energyState = runtime.subscriptionRef(Effect.gen(function*() {
  return (yield* Thermostat).energy.state
}))
const setpointState = runtime.subscriptionRef(Effect.gen(function*() {
  return (yield* Thermostat).setpoint.state
}))
const energy = Atom.map(energyState, AsyncResult.map(Crdt.PNCounter.value))
const setpoint = Atom.map(setpointState, AsyncResult.map(Setpoint.value))
const addEnergy = runtime.fn((by: number) =>
  Effect.gen(function*() {
    yield* (yield* Thermostat).energy.update((s, me) => Crdt.PNCounter.add(s, me, by))
  }), { concurrent: true })
const setSetpoint = runtime.fn((value: number) =>
  Effect.gen(function*() {
    yield* (yield* Thermostat).setpoint.update((s, me) => Setpoint.set(s, me, value))
  }), { concurrent: true })

// ── wiring: Replica over iroh ────────────────────────────────────────────────

const ALPN = "example/replica-sync/1"
const names = ["X", "Y", "Z"]

const node = (endpoint: IrohEndpoint.IrohEndpoint["Service"], all: ReadonlyArray<IrohEndpoint.IrohEndpoint["Service"]>) =>
  Effect.gen(function*() {
    const ids = all.map((e) => e.id)
    const transport = Layer.mergeAll(Layer.succeed(IrohEndpoint.IrohEndpoint)(endpoint), RpcSerialization.layerNdjson)
    const replica = yield* Layer.build(Layer.unwrap(Effect.gen(function*() {
      const client = yield* IrohRpc.makePeerClients(Replica.ReplicaSync, {
        alpn: ALPN,
        resolve: (id) => Effect.succeed(all.find((e) => e.id === id) ?? { id, addresses: [] })
      })
      return Replica.layer({ self: endpoint.id, peers: () => ids.filter((id) => id !== endpoint.id), client })
    })).pipe(Layer.provide(transport)))

    yield* Layer.build(RpcServer.layer(Replica.ReplicaSync).pipe(
      Layer.provide(Replicated.layerHandlers),
      Layer.provide(IrohRpc.layerProtocolServer({ alpn: ALPN, allow: (peer) => ids.includes(peer) })),
      Layer.provide(transport),
      Layer.provide(Layer.succeedContext(replica))
    ))
    const service = yield* Layer.build(ThermostatLive.pipe(Layer.provide(Layer.succeedContext(replica))))
    const registry = AtomRegistry.make({ initialValues: [[runtime.layer, Layer.succeedContext(service)]] })
    const mounted: ReadonlyArray<Atom.Atom<unknown>> = [energy, setpoint, addEnergy, setSetpoint]
    for (const atom of mounted) registry.mount(atom)
    return registry
  })

// ── run ──────────────────────────────────────────────────────────────────────

const rows: Array<Record<string, unknown>> = []
const snapshot = (step: string, registries: ReadonlyArray<AtomRegistry.AtomRegistry>) =>
  registries.forEach((r, i) => {
    const e = r.get(energy)
    const s = r.get(setpoint)
    rows.push({
      step,
      replica: names[i],
      energy: AsyncResult.isSuccess(e) ? e.value : "…",
      setpoint: AsyncResult.isSuccess(s) ? s.value.value : "…",
      candidates: AsyncResult.isSuccess(s) ? s.value.candidates.join(", ") : "…"
    })
  })

await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  // Endpoints bind port 0 like HttpServer; peers dial what each one reports.
  const endpoints = yield* Effect.forEach(names, () =>
    Effect.map(
      Layer.build(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() })),
      (ctx) => Context.get(ctx, IrohEndpoint.IrohEndpoint)
    ))
  endpoints.forEach((e, i) =>
    console.log(`${names[i]} ${e.id.slice(0, 10)} @ ${e.addresses.map(NetAddress.formatInet).join(" ")}`))
  const registries = yield* Effect.forEach(endpoints, (e) => node(e, endpoints))
  yield* Effect.sleep("500 millis")

  // Element code: only atom get/set.
  for (let n = 0; n < 100; n++) {
    for (const r of registries) r.set(addEnergy, 1)
    yield* Effect.sleep("5 millis")
  }
  registries.forEach((r, i) => r.set(setSetpoint, [19, 22, 21][i]!)) // concurrent, conflicting
  yield* Effect.sleep("1 second")
  snapshot("concurrent writes", registries)

  registries[0]!.set(setSetpoint, 20) // X has seen all three: supersedes them
  yield* Effect.sleep("1 second")
  snapshot("X writes 20", registries)
})))
console.table(rows)
process.exit(0)
