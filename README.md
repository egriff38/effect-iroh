# effect-iroh

> **Experimental.** APIs will change. Not published to npm yet.

[iroh](https://iroh.computer) (QUIC, dial by public key, NAT traversal) as a transport for [Effect](https://effect.website): endpoints as layers, iroh streams as Effect `Socket`s, Effect RPC (and so `AtomRpc`) over iroh.

| Package | What it is | Depends on |
|---|---|---|
| [`effect-iroh`](packages/effect-iroh) | `IrohEndpoint` as a scoped layer (in the shape of `HttpServer`), iroh streams as Effect `Socket`s, Effect RPC client/server protocols, a per-peer client cache | `effect`, `@number0/iroh` |
| [`effect-replicated`](packages/effect-replicated) | Services whose fields are state-based CRDTs, replicated over **any** Effect RPC protocol. PN-counter and multi-value register with a pluggable resolver | `effect` |

They are separate on purpose: `effect-replicated` doesn't know about iroh (its tests run in memory with `RpcTest`), and `effect-iroh` doesn't know about atoms or CRDTs. Everything else composes from Effect itself: `AtomRpc` for request/response atoms, `runtime.subscriptionRef` for atoms over replicated fields.

## Examples

| Example | Shows |
|---|---|
| [`examples/atomrpc-thermostat`](examples/atomrpc-thermostat) | Provider/consumer with only `AtomRpc` + `effect-iroh`: a typed `RpcGroup`, a live stream atom, a mutation |
| [`examples/crdt-thermostat`](examples/crdt-thermostat) | Three replicas over a private iroh network: concurrent increments converge exactly, conflicting writes resolve identically, a later write supersedes them. Also a Tern notebook |

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

For CRDT-replicated services over the same transport, see [`examples/crdt-thermostat`](examples/crdt-thermostat) and [`effect-replicated`](packages/effect-replicated).

## Known issues

- **iroh JS bindings:** `RecvStream.stop()` waits for the same lock as a pending `read()`, so awaiting it from a stream `cancel` deadlocks; `effect-iroh` doesn't await it ([n0-computer/iroh-ffi#309](https://github.com/n0-computer/iroh-ffi/issues/309)). Bytes cross the binding as `Array<number>`, one copy each way ([n0-computer/iroh-ffi#276](https://github.com/n0-computer/iroh-ffi/issues/276)). Node only (napi); no gossip, blobs or docs from JS.
- iroh binds an IPv6 socket on `[::]` alongside the requested one; it appears in `IrohEndpoint.addresses` but isn't a dialable address.
- Replication sends a field's full state on every change and assumes a full mesh. Fine for small clusters; deltas and gossip are future work.

## License

MIT
