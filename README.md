# effect-iroh

> **Experimental.** APIs will change. Not published to npm yet.

[iroh](https://iroh.computer) (QUIC, dial by public key, NAT traversal) as a transport for [Effect](https://effect.website): endpoints as layers, iroh streams as Effect `Socket`s, Effect RPC (and so `AtomRpc`) over iroh.

The core is one package, [`effect-iroh`](packages/effect-iroh) (depends on `effect` and `@number0/iroh`). It doesn't know about atoms or CRDTs; those compose from Effect itself (`AtomRpc`, `runtime.subscriptionRef`) and live in examples.

## User stories, one example each

| Example | Story | iroh primitive |
|---|---|---|
| [`atomrpc-thermostat`](examples/atomrpc-thermostat) | A consumer drives a provider's state as atoms: a live stream atom and a mutation, with only `AtomRpc` + `effect-iroh` | ALPN protocols, dial by key |
| [`pair-by-ticket`](examples/pair-by-ticket) | Two machines pair with a ticket wherever they are, and the UI shows whether the link is direct or relayed, live | n0 relays + address lookup, tickets, path watching, hole punching |
| [`parallel-streams`](examples/parallel-streams) | A big transfer doesn't stall interactive calls | one QUIC stream per RPC call |
| [`web-chat`](examples/web-chat) | Browsers join a room through whichever node served them; nodes reach the room's host over iroh | the same `RpcGroup` over WebSocket and iroh |
| [`crdt-thermostat`](examples/crdt-thermostat) | Replicas converge without a coordinator (CRDT [example code](examples/crdt-thermostat/src/replicated), not a package). Also a Tern notebook | full mesh of peers |

```sh
pnpm install
pnpm --filter example-atomrpc-thermostat start
pnpm --filter example-pair-by-ticket start demo        # needs internet: n0 relays
pnpm --filter example-parallel-streams start
pnpm --filter example-web-chat start                   # then open http://localhost:4300 and :4301
```

Measured on one laptop (localhost):

| | Result |
|---|---|
| `pair-by-ticket demo` (relay-first dial) | relay path at ~136 ms (rtt ~123 ms), direct path at ~335 ms |
| `parallel-streams` (64 MB in 1 MB chunks, pings every 10 ms) | `"shared"`: one ping waited ~6.8 s; `"per-request"`: p50 ~10 ms, worst ~1.3 s |

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

The web-chat browser client is plain Effect RPC over a WebSocket (`RpcClient.layerProtocolSocket` + `Socket.layerWebSocket`); browsers can't run iroh themselves (see below).

## Known issues

- **iroh JS bindings:** `RecvStream.stop()` waits for the same lock as a pending `read()`, so awaiting it from a stream `cancel` deadlocks; `effect-iroh` doesn't await it ([n0-computer/iroh-ffi#309](https://github.com/n0-computer/iroh-ffi/issues/309)). Bytes cross the binding as `Array<number>`, one copy each way ([n0-computer/iroh-ffi#276](https://github.com/n0-computer/iroh-ffi/issues/276)), which caps throughput (tens of MB/s) and stalls the JS thread on large chunks. `watchPaths` is typed `(paths) => void` but called error-first, `(err, paths)`.
- **Browsers:** the JS package is Node-only (napi). iroh's wasm build is relay-only and has no npm package; until there is one, browsers reach iroh through a node, as in `web-chat`.
- iroh binds an IPv6 socket on `[::]` alongside the requested one; it appears in `IrohEndpoint.addresses` but isn't a dialable address.
- `crdt-thermostat` sends a field's full state on every change and assumes a full mesh.

## License

MIT
