/**
 * Connection health as Effect streams: which paths a connection has (direct UDP or relay), which
 * one is selected, and its round-trip time. Run one on an atom runtime to get a live health atom.
 */
import type * as I from "@number0/iroh"
import { Effect, Queue, Stream, SubscriptionRef } from "effect"
import { IrohEndpoint } from "./IrohEndpoint.js"

export interface Path {
  readonly id: string
  readonly kind: "direct" | "relay"
  readonly selected: boolean
  /** `ip:port` for direct paths, the relay URL for relay paths. */
  readonly remote: string
  readonly rttMs: number
  readonly lostPackets: number
}

export interface Health {
  /** The selected path's kind, or `undefined` while no path is selected. */
  readonly via: Path["kind"] | undefined
  readonly rttMs: number | undefined
  readonly paths: ReadonlyArray<Path>
}

const toPath = (p: I.PathSnapshot): Path => ({
  id: p.id,
  kind: p.isRelay ? "relay" : "direct",
  selected: p.isSelected,
  remote: p.remoteAddr,
  rttMs: p.rttMs,
  lostPackets: p.stats.lostPackets
})

const toHealth = (snapshots: ReadonlyArray<I.PathSnapshot>): Health => {
  const paths = snapshots.map(toPath)
  const selected = paths.find((p) => p.selected)
  return { via: selected?.kind, rttMs: selected?.rttMs, paths }
}

/** `conn`'s paths now and on every change, until the stream is interrupted. */
export const health = (conn: I.Connection): Stream.Stream<Health> =>
  Stream.callback<Health>((queue) =>
    Effect.gen(function*() {
      Queue.offerUnsafe(queue, toHealth(conn.paths()))
      // Typed `(paths) => void`, but napi calls it error-first: `(err, paths)`.
      const onPaths = (...args: ReadonlyArray<unknown>) => {
        const paths = args.at(-1)
        if (Array.isArray(paths)) Queue.offerUnsafe(queue, toHealth(paths as ReadonlyArray<I.PathSnapshot>))
      }
      const handle = conn.watchPaths(onPaths)
      yield* Effect.addFinalizer(() => Effect.promise(() => handle.stop()))
    }), { bufferSize: 1, strategy: "sliding" })

/**
 * Health of whatever connection the context's endpoint currently has to `peer`; `undefined`
 * while there is none. Follows reconnects.
 */
export const healthOf = (peer: string): Stream.Stream<Health | undefined, never, IrohEndpoint> =>
  Stream.unwrap(Effect.gen(function*() {
    const ep = yield* IrohEndpoint
    return SubscriptionRef.changes(ep.connections).pipe(
      Stream.map((m) => m.get(peer)),
      Stream.changes,
      Stream.switchMap((conn) => conn === undefined ? Stream.make(undefined) : health(conn))
    )
  }))
