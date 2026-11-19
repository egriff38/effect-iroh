# effect-replicated

> **Experimental.**

Effect services whose fields are state-based CRDTs, replicated over any Effect RPC protocol.

- `Crdt`: the `Crdt<S, V>` interface (`schema`, `empty`, `merge`, `value`), `PNCounter`, and `MVRegister(schema, resolve)`, which keeps concurrent writes and resolves them on read.
- `Replica`: the context: `self`, `peers(service)`, `client(peer)`, and the `ReplicaSync` RPC group replicas speak. `Replica.layerOffline` replicates with nobody.
- `Replicated.layer(Tag, crdts)`: builds a service of `Replica.Field`s (each a `SubscriptionRef` plus local `update`), pulling and merging every peer's state. `Replicated.layerHandlers` serves this replica's fields; provide it to `RpcServer.layer(Replica.ReplicaSync)` with any protocol.

Element code reads and writes the fields' `SubscriptionRef`s, or atoms over them with `runtime.subscriptionRef`. The transport is whatever `client` and the server protocol are: see `examples/crdt-thermostat` for iroh, and `test/Replicated.test.ts` for in-memory `RpcTest`.

Not solved here: invariants (route those writes to an owner), garbage collection of clocks, persistence, and delta sync.
