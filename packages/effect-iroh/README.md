# effect-iroh

> **Experimental.**

[iroh](https://iroh.computer) as an Effect transport.

- `IrohEndpoint.layer({ secretKey, network?, bind? })` / `layerConfig(...)`: an endpoint as a scoped service. `network` is `"private"` (default: no relays, no discovery), `"n0"` (public relays + DNS address lookup) or `{ relays }`. Reports bound `addresses` as `NetAddress.InetAddress`, its current `address` for peers, `online`, open `connections` by peer, and routes incoming connections by ALPN.
- `IrohTicket.PeerAddressFromTicket` / `encode`: endpoint tickets as a Schema, so a peer can come from Config, a CLI argument or a QR code.
- `IrohConnection.health(conn)` / `healthOf(peer)`: paths (direct or relay), the selected one and RTT as a `Stream`; run it on an atom runtime for a live health atom.
- `IrohRpc.layerProtocolClient(peer, alpn, { streams? })` / `layerProtocolServer({ alpn, allow })`: Effect RPC over iroh, usable with `RpcClient`, `RpcServer` and `AtomRpc`. `streams: "per-request"` (default) gives every call its own QUIC stream; `"shared"` uses one framed stream.
- `IrohRpc.makePeerClients(group, { alpn, resolve })`: typed clients keyed by EndpointId, one connection per peer.
- `IrohSocket.makeSocket(peer, alpn)` / `layerSocketServer({ alpn, allow })`: iroh streams as Effect `Socket` / `SocketServer`. `allow` checks the TLS-authenticated EndpointId; every stream a peer opens becomes its own socket.

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
