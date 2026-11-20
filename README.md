# effect-iroh

> **Experimental.** APIs will change. Not published to npm yet.

[iroh](https://iroh.computer) (QUIC, dial by public key, NAT traversal) as a transport for [Effect](https://effect.website): endpoints as layers, iroh streams as Effect `Socket`s, Effect RPC (and so `AtomRpc`) over iroh.

The core is one package, [`effect-iroh`](packages/effect-iroh) (depends on `effect` and `@number0/iroh`): `IrohEndpoint` as a scoped layer (in the shape of `HttpServer`), iroh streams as Effect `Socket`s, Effect RPC client/server protocols, a per-peer client cache. It doesn't know about atoms or CRDTs; those compose from Effect itself (`AtomRpc`, `runtime.subscriptionRef`) and live in examples.

## Examples

| Example | Shows |
|---|---|
| [`examples/atomrpc-thermostat`](examples/atomrpc-thermostat) | Provider/consumer with only `AtomRpc` + `effect-iroh`: a typed `RpcGroup`, a live stream atom, a mutation |
| [`examples/crdt-thermostat`](examples/crdt-thermostat) | State-based CRDT services ([example code](examples/crdt-thermostat/src/replicated), not a package) replicated over iroh: concurrent increments converge exactly, conflicting writes resolve identically, a later write supersedes them. Also a Tern notebook |

```sh
pnpm install
pnpm --filter example-atomrpc-thermostat start
```

## Quick look

An ordinary `RpcGroup`, served over iroh and consumed as atoms with Effect's own `AtomRpc`:

```ts
class ThermostatRpcs extends RpcGroup.make(
  Rpc.make("Temperature", { success: Schema.Number, stream: true }),
  Rpc.make("SetTemperature", { payload: { value: Schema.Number } })
) {}

// provider: handlers over a SubscriptionRef, on an iroh endpoint
RpcServer.layer(ThermostatRpcs).pipe(
  Layer.provide(ThermostatRpcs.toLayer({
    Temperature: () => SubscriptionRef.changes(temperature),
    SetTemperature: ({ value }) => SubscriptionRef.set(temperature, value)
  })),
  Layer.provide(IrohRpc.layerProtocolServer({ alpn: "example/thermostat/1" })),
  Layer.provide(RpcSerialization.layerNdjson),
  Layer.provide(IrohEndpoint.layer({ secretKey }))
)

// consumer: AtomRpc whose protocol dials the provider by its key
class Thermostat extends AtomRpc.Service<Thermostat>()("example/Thermostat", {
  group: ThermostatRpcs,
  protocol: IrohRpc.layerProtocolClient(provider, "example/thermostat/1").pipe(
    Layer.provide(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() })),
    Layer.provide(RpcSerialization.layerNdjson)
  )
}) {}

const temperature = Thermostat.runtime.atom(Stream.unwrap(Effect.gen(function*() {
  return (yield* Thermostat)("Temperature", undefined) // a live atom, not a paged query
})))
const setTemperature = Thermostat.mutation("SetTemperature")
```

For CRDT-replicated services over the same transport, see [`examples/crdt-thermostat`](examples/crdt-thermostat).

## Known issues

- **iroh JS bindings:** `RecvStream.stop()` waits for the same lock as a pending `read()`, so awaiting it from a stream `cancel` deadlocks; `effect-iroh` doesn't await it ([n0-computer/iroh-ffi#309](https://github.com/n0-computer/iroh-ffi/issues/309)). Bytes cross the binding as `Array<number>`, one copy each way ([n0-computer/iroh-ffi#276](https://github.com/n0-computer/iroh-ffi/issues/276)). Node only (napi); no gossip, blobs or docs from JS.
- iroh binds an IPv6 socket on `[::]` alongside the requested one; it appears in `IrohEndpoint.addresses` but isn't a dialable address.
- Replication sends a field's full state on every change and assumes a full mesh. Fine for small clusters; deltas and gossip are future work.

## License

MIT
