/**
 * The replication context: who this replica is, who else holds a service, how to reach them,
 * and the one RPC replicas speak. Transport-agnostic: `client` can be iroh, a WebSocket,
 * a worker or an in-memory test client.
 */
import { Context, Effect, Layer, Schema } from "effect"
import type { Scope, SubscriptionRef } from "effect"
import { Rpc, RpcGroup } from "effect/rpc"
import type { RpcClient, RpcClientError } from "effect/rpc"
import type { Crdt } from "./Crdt.js"

export class ReplicaError extends Schema.TaggedError<ReplicaError>()("ReplicaError", {
  key: Schema.String,
  reason: Schema.String
}) {}

/** Each replica serves its own state; peers pull it and merge. */
export class ReplicaSync extends RpcGroup.make(
  Rpc.make("Watch", { payload: { key: Schema.String }, success: Schema.Json, error: ReplicaError, stream: true })
) {}

export type ReplicaSyncClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof ReplicaSync>, RpcClientError.RpcClientError>

/** One replicated field: local CRDT state plus how to merge and read it. */
export interface Field<S, V> {
  readonly state: SubscriptionRef.SubscriptionRef<S>
  readonly crdt: Crdt<S, V>
  /** A local op; `self` is this replica's id. */
  readonly update: (f: (s: S, self: string) => S) => Effect.Effect<void>
}

export interface Options {
  /** This replica's id; with iroh, its EndpointId. */
  readonly self: string
  /** The other replicas holding `service`. */
  readonly peers: (service: string) => ReadonlyArray<string>
  readonly client: (peer: string) => Effect.Effect<ReplicaSyncClient, never, Scope.Scope>
}

export class Replica extends Context.Service<Replica, Options & {
  /** Fields this replica serves, by `<service key>/<field>`. */
  readonly fields: Map<string, Field<unknown, unknown>>
}>()("effect-replicated/Replica") {}

export const layer = (options: Options): Layer.Layer<Replica> =>
  Layer.sync(Replica, () => Replica.of({ ...options, fields: new Map() }))

/** No peers: the same CRDTs, local-only. A good default context for atoms. */
export const layerOffline: Layer.Layer<Replica> = layer({
  self: "local",
  peers: () => [],
  client: () => Effect.die("effect-replicated: offline replica has no peers")
})
