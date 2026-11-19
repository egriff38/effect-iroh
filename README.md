# effect-iroh

> **Experimental.** APIs will change. Not published to npm yet.

[iroh](https://iroh.computer) (QUIC, dial by public key, NAT traversal) as a transport for [Effect](https://effect.website), plus CRDT-replicated services that compose with Effect's own atoms.

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
pnpm --filter example-crdt-thermostat start
```

## Element code

```ts
class Thermostat extends Context.Service<Thermostat, {
  readonly energy: Replica.Field<Crdt.PNCounterState, number>
  readonly setpoint: Replica.Field<Crdt.MVRegisterState<number>, Crdt.Resolved<number>>
}>()("acme/Thermostat") {}

const Setpoint = Crdt.MVRegister(Schema.Number, (concurrent) => Math.max(...concurrent)) // resolver
const runtime = Atom.runtime(
  Replicated.layer(Thermostat, { energy: Crdt.PNCounter, setpoint: Setpoint })
    .pipe(Layer.provide(Replica.layerOffline)) // nodes swap in a networked Replica
)
const energy = Atom.map(
  runtime.subscriptionRef(Effect.gen(function*() { return (yield* Thermostat).energy.state })),
  AsyncResult.map(Crdt.PNCounter.value)
)
```

## Known issues

- **iroh JS bindings:** `RecvStream.stop()` waits for the same lock as a pending `read()`, so awaiting it from a stream `cancel` deadlocks; `effect-iroh` doesn't await it ([n0-computer/iroh-ffi#309](https://github.com/n0-computer/iroh-ffi/issues/309)). Bytes cross the binding as `Array<number>`, one copy each way ([n0-computer/iroh-ffi#276](https://github.com/n0-computer/iroh-ffi/issues/276)). Node only (napi); no gossip, blobs or docs from JS.
- iroh binds an IPv6 socket on `[::]` alongside the requested one; it appears in `IrohEndpoint.addresses` but isn't a dialable address.
- Replication sends a field's full state on every change and assumes a full mesh. Fine for small clusters; deltas and gossip are future work.

## License

MIT
