/**
 * Does a big transfer delay unrelated calls? Measures `Ping` round trips while a large `Download`
 * streams on the same connection, once with every call sharing one QUIC stream (`"shared"`) and
 * once with one QUIC stream per call (`"per-request"`, the default).
 *
 *   bun run src/main.ts [megabytes=64] [chunkKb=1024]
 */
import { Effect, Layer, Schema, Stream } from "effect"
import { RpcSerialization, RpcServer, Rpc, RpcGroup } from "effect/rpc"
import { IrohEndpoint, IrohRpc } from "effect-iroh"

class BenchRpcs extends RpcGroup.make(
  Rpc.make("Ping", { success: Schema.Number }),
  Rpc.make("Download", { payload: { chunks: Schema.Number, chunkKb: Schema.Number }, success: Schema.String, stream: true })
) {}

const ALPN = "example/bench/1"
const [megabytes = 64, chunkKb = 1024] = process.argv.slice(2).map(Number)
const chunks = Math.ceil((megabytes * 1024) / chunkKb)
// Chunks are single large ndjson frames; lift the default 16 MiB incomplete-frame cap.
const serialization = RpcSerialization.layerNdjsonWith({ maxBufferSize: "unbounded" })

const server = Effect.gen(function*() {
  const ctx = yield* Layer.build(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() }))
  yield* Layer.build(RpcServer.layer(BenchRpcs).pipe(
    Layer.provide(BenchRpcs.toLayer({
      Ping: () => Effect.sync(() => Date.now()),
      Download: ({ chunks, chunkKb }) => {
        const chunk = "x".repeat(chunkKb * 1024)
        return Stream.range(1, chunks).pipe(Stream.map(() => chunk))
      }
    })),
    Layer.provide(IrohRpc.layerProtocolServer({ alpn: ALPN })),
    Layer.provide(serialization),
    Layer.provide(Layer.succeedContext(ctx))
  ))
  return yield* IrohEndpoint.IrohEndpoint.pipe(Effect.provide(ctx))
})

const percentile = (sorted: ReadonlyArray<number>, p: number) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] ?? NaN

const measure = (peer: IrohEndpoint.PeerAddress, streams: "shared" | "per-request") =>
  Effect.gen(function*() {
    const clients = yield* IrohRpc.makePeerClients(BenchRpcs, { alpn: ALPN, streams, resolve: () => Effect.succeed(peer) })
    const client = yield* clients(peer.id)
    yield* client.Ping() // connect before timing

    const idle: Array<number> = []
    for (let i = 0; i < 20; i++) {
      const t = performance.now()
      yield* client.Ping()
      idle.push(performance.now() - t)
    }

    const busy: Array<number> = []
    let done = false
    const started = performance.now()
    const download = Stream.runCount(client.Download({ chunks, chunkKb })).pipe(
      Effect.ensuring(Effect.sync(() => (done = true)))
    )
    const pings = Effect.gen(function*() {
      while (!done) {
        const t = performance.now()
        yield* client.Ping()
        busy.push(performance.now() - t)
        yield* Effect.sleep("10 millis")
      }
    })
    yield* Effect.all([download, pings], { concurrency: "unbounded" })
    const seconds = (performance.now() - started) / 1000

    const fmt = (xs: Array<number>) => {
      const s = [...xs].sort((a, b) => a - b)
      return `p50 ${percentile(s, 0.5).toFixed(1)}  p95 ${percentile(s, 0.95).toFixed(1)}  max ${(s.at(-1) ?? NaN).toFixed(1)}`
    }
    return {
      streams,
      "idle ping ms": fmt(idle),
      "ping during download ms": fmt(busy),
      pings: busy.length,
      "download MB/s": (megabytes / seconds).toFixed(0)
    }
  }).pipe(
    Effect.scoped,
    Effect.provide(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() })),
    Effect.provide(serialization)
  )

await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const ep = yield* server
  const peer = { id: ep.id, addresses: ep.addresses }
  console.log(`${megabytes} MB in ${chunks} chunks of ${chunkKb} KB, pinging every 10 ms during the download`)
  const rows = []
  for (const mode of ["shared", "per-request"] as const) rows.push(yield* measure(peer, mode))
  console.table(rows)
})))
process.exit(0)
