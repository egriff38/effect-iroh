/**
 * Services whose fields are CRDTs, replicated through the context's `Replica`. Element code
 * sees ordinary `SubscriptionRef`s (and atoms over them via `runtime.subscriptionRef`).
 */
import { Context, Effect, Layer, Schedule, Schema, Stream, SubscriptionRef } from "effect"
import type { Crdt } from "./Crdt.js"
import { Replica, ReplicaError, ReplicaSync } from "./Replica.js"
import type { Field } from "./Replica.js"

/** Maps a service shape to the CRDT each field needs. */
export type Crdts<S> = {
  readonly [K in keyof S]: S[K] extends Field<infer St, infer V> ? Crdt<St, V> : never
}

/** Builds `tag` from CRDTs, pulls every peer's state for each field and merges it in. */
export const layer = <Id, S extends object>(tag: Context.Key<Id, S>, crdts: Crdts<S>): Layer.Layer<Id, never, Replica> =>
  Layer.effect(tag, Effect.gen(function*() {
    const replica = yield* Replica
    // Field types are erased here and restored by Crdts<S> at the boundary.
    const entries = Object.entries(crdts) as Array<[string, Crdt<unknown, unknown>]>
    const fields = yield* Effect.forEach(entries, ([name, crdt]) =>
      Effect.gen(function*() {
        const key = `${tag.key}/${name}`
        const state = yield* SubscriptionRef.make(crdt.empty)
        const decode = Schema.decodeUnknownEffect(Schema.toCodecJson(crdt.schema))
        for (const peer of replica.peers(tag.key)) {
          yield* Stream.unwrap(Effect.map(replica.client(peer), (client) => client.Watch({ key }))).pipe(
            Stream.mapEffect((u) => decode(u)),
            Stream.retry(Schedule.spaced("500 millis")),
            Stream.runForEach((remote) => SubscriptionRef.update(state, (local) => crdt.merge(local, remote))),
            Effect.forkScoped
          )
        }
        const field: Field<unknown, unknown> = {
          state,
          crdt,
          update: (f) => SubscriptionRef.update(state, (s) => f(s, replica.self))
        }
        replica.fields.set(key, field)
        yield* Effect.addFinalizer(() => Effect.sync(() => replica.fields.delete(key)))
        return [name, field] as const
      }))
    return Object.fromEntries(fields) as unknown as S
  }))

/**
 * Handlers serving this replica's fields. Provide to `RpcServer.layer(ReplicaSync)` with any
 * server protocol. Streams conflate to the latest state: safe because later states dominate.
 */
export const layerHandlers = ReplicaSync.toLayer(Effect.gen(function*() {
  const replica = yield* Replica
  return ReplicaSync.of({
    Watch: ({ key }) => {
      const field = replica.fields.get(key)
      if (field === undefined) return Stream.fail(new ReplicaError({ key, reason: "not replicated here" }))
      const encode = Schema.encodeEffect(Schema.toCodecJson(field.crdt.schema))
      return SubscriptionRef.changes(field.state).pipe(
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.mapEffect((s) => Effect.mapError(encode(s), (e) => new ReplicaError({ key, reason: e.message })))
      )
    }
  })
}))
