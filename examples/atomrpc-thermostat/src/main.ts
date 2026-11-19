/**
 * Provider/consumer over iroh with nothing but Effect's own AtomRpc: a typed RpcGroup,
 * handlers over a SubscriptionRef, and an AtomRpc client whose protocol is effect-iroh's.
 * Both endpoints run in this process so the example is one command.
 */
import { Effect, Layer, Schema, Stream, SubscriptionRef } from "effect"
import { AsyncResult, AtomRegistry, AtomRpc } from "effect/reactivity"
import { Rpc, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import { IrohEndpoint, IrohRpc } from "effect-iroh"

// ---------- the contract: an ordinary RpcGroup ----------

class ThermostatRpcs extends RpcGroup.make(
  Rpc.make("Temperature", { success: Schema.Number, stream: true }),
  Rpc.make("SetTemperature", { payload: { value: Schema.Number } })
) {}

const ALPN = "example/thermostat/1"

// ---------- provider: state in a SubscriptionRef, served over iroh ----------

const provider = Effect.gen(function*() {
  const temperature = yield* SubscriptionRef.make(20)
  yield* Effect.forkScoped(Effect.forever(
    Effect.sleep("200 millis").pipe(Effect.andThen(SubscriptionRef.update(temperature, (t) => t + 1)))
  ))
  const handlers = ThermostatRpcs.toLayer({
    Temperature: () => SubscriptionRef.changes(temperature),
    SetTemperature: ({ value }) => SubscriptionRef.set(temperature, value)
  })
  const ctx = yield* Layer.build(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() }))
  yield* Layer.build(RpcServer.layer(ThermostatRpcs).pipe(
    Layer.provide(handlers),
    Layer.provide(IrohRpc.layerProtocolServer({ alpn: ALPN })),
    Layer.provide(RpcSerialization.layerNdjson),
    Layer.provide(Layer.succeedContext(ctx))
  ))
  return yield* IrohEndpoint.IrohEndpoint.pipe(Effect.provide(ctx))
})

// ---------- consumer: AtomRpc, protocol = effect-iroh ----------

const consumer = (peer: IrohEndpoint.PeerAddress) => {
  class Thermostat extends AtomRpc.Service<Thermostat>()("example/Thermostat", {
    group: ThermostatRpcs,
    protocol: IrohRpc.layerProtocolClient(peer, ALPN).pipe(
      Layer.provide(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() })),
      Layer.provide(RpcSerialization.layerNdjson)
    )
  }) {}

  return {
    // AtomRpc.query on a stream RPC is pull-based (pages); for "current value" run the stream
    // on the same AtomRpc runtime instead.
    temperature: Thermostat.runtime.atom(Stream.unwrap(Effect.gen(function*() {
      const client = yield* Thermostat
      return client("Temperature", undefined)
    }))),
    setTemperature: Thermostat.mutation("SetTemperature")
  }
}

// ---------- run ----------

await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const ep = yield* provider
  console.log(`provider ${ep.id.slice(0, 10)} listening on iroh`)
  const atoms = consumer({ id: ep.id, addresses: ep.addresses })
  const registry = AtomRegistry.make()

  yield* Effect.callback<void>((resume) => {
    let wrote = false
    const unsubscribe = registry.subscribe(atoms.temperature, (t) => {
      if (!AsyncResult.isSuccess(t)) return console.log(`consumer temperature=${t._tag}`)
      console.log(`consumer temperature=${t.value}`)
      if (!wrote && t.value >= 22) {
        wrote = true
        // From outside the notification, as a UI event handler would.
        setTimeout(() => registry.set(atoms.setTemperature, { payload: { value: 1000 } }))
      } else if (wrote && t.value > 1000) {
        unsubscribe()
        resume(Effect.void)
      }
    }, { immediate: true })
  })
  console.log("the consumer's mutation reached the provider, which kept ticking from it")
})))
process.exit(0)
