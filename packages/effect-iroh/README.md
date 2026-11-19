# effect-iroh

> **Experimental.**

[iroh](https://iroh.computer) as an Effect transport.

- `IrohEndpoint.layer({ secretKey, bind? })` / `layerConfig(...)`: a private endpoint (no n0 discovery, no relays) as a scoped service. Reports its bound `addresses` as `NetAddress.InetAddress`, routes incoming connections by ALPN.
- `IrohSocket.makeSocket(peer, alpn)` / `layerSocketServer({ alpn, allow })`: iroh streams as Effect `Socket` / `SocketServer`. `allow` checks the TLS-authenticated EndpointId.
- `IrohRpc.layerProtocolClient(peer, alpn)` / `layerProtocolServer({ alpn, allow })`: Effect RPC over iroh, usable with `RpcClient`, `RpcServer` and `AtomRpc`.
- `IrohRpc.makePeerClients(group, { alpn, resolve })`: typed clients keyed by EndpointId, one connection per peer.

```ts
const server = RpcServer.layer(MyRpcs).pipe(
  Layer.provide(MyHandlers),
  Layer.provide(IrohRpc.layerProtocolServer({ alpn: "my-app/1", allow: (peer) => members.has(peer) })),
  Layer.provide(RpcSerialization.layerNdjson),
  Layer.provide(IrohEndpoint.layerConfig({
    secretKey: Config.Redacted("IROH_SECRET_KEY"),
    bind: Config.schema(Schema.InetAddressFromString, "IROH_BIND")
  }))
)
```

See the [repository README](../../README.md) for examples and known issues.
