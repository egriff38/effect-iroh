import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer, Schema, Stream } from "effect"
import type { Scope } from "effect"
import { NetAddress } from "effect/net"
import { Rpc, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import type { RpcClient, RpcClientError } from "effect/rpc"
import { IrohEndpoint, IrohRpc } from "../src/index.js"

class Echo extends RpcGroup.make(
  Rpc.make("Echo", { payload: { text: Schema.String }, success: Schema.String }),
  Rpc.make("Count", { payload: { to: Schema.Number }, success: Schema.Number, stream: true })
) {}

const EchoHandlers = Echo.toLayer({
  Echo: ({ text }) => Effect.succeed(text),
  Count: ({ to }) => Stream.range(1, to)
})

const ALPN = "effect-iroh/test/0"

type EchoClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof Echo>, RpcClientError.RpcClientError>
type Clients = (peer: string) => Effect.Effect<EchoClient, never, Scope.Scope>

/** A bound endpoint plus an Echo server on it, alive for the test's scope. */
const server = (allow?: (peer: string) => boolean) =>
  Effect.gen(function*() {
    const ctx = yield* Layer.build(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() }))
    yield* Layer.build(
      RpcServer.layer(Echo).pipe(
        Layer.provide(EchoHandlers),
        Layer.provide(IrohRpc.layerProtocolServer({ alpn: ALPN, allow })),
        Layer.provide(RpcSerialization.layerNdjson),
        Layer.provide(Layer.succeedContext(ctx))
      )
    )
    const ep = Context.get(ctx, IrohEndpoint.IrohEndpoint)
    return { id: ep.id, addresses: ep.addresses } satisfies IrohEndpoint.PeerAddress
  })

const client = <A, E>(f: (clients: Clients) => Effect.Effect<A, E, Scope.Scope>, peers: ReadonlyArray<IrohEndpoint.PeerAddress>) =>
  Effect.gen(function*() {
    const clients = yield* IrohRpc.makePeerClients(Echo, {
      alpn: ALPN,
      resolve: (id) => Effect.succeed(peers.find((p) => p.id === id) ?? { id, addresses: [] })
    })
    return yield* f(clients)
  }).pipe(
    Effect.provide(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() })),
    Effect.provide(RpcSerialization.layerNdjson)
  )

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) => Effect.runPromise(Effect.scoped(effect))

describe("IrohEndpoint", () => {
  test("binds an ephemeral port and reports the resolved address", () =>
    run(Effect.gen(function*() {
      const peer = yield* server()
      const v4 = peer.addresses.find(NetAddress.isInetAddressV4)
      expect(v4?.port).toBeGreaterThan(0)
      expect(peer.id).toMatch(/^[0-9a-f]{64}$/)
    })))
})

describe("IrohRpc", () => {
  test("unary and streaming calls round-trip over iroh", () =>
    run(Effect.gen(function*() {
      const peer = yield* server()
      const [echoed, counted] = yield* client((clients) =>
        Effect.gen(function*() {
          const echo = yield* clients(peer.id)
          return [
            yield* echo.Echo({ text: "hello" }),
            yield* Stream.runCollect(echo.Count({ to: 3 }))
          ] as const
        }), [peer])
      expect(echoed).toBe("hello")
      expect(counted).toEqual([1, 2, 3])
    })))

  test("a peer outside `allow` gets no response", () =>
    run(Effect.gen(function*() {
      const peer = yield* server(() => false)
      const result = yield* client((clients) =>
        Effect.flatMap(clients(peer.id), (echo) => echo.Echo({ text: "let me in" })).pipe(
          Effect.timeoutOption("2 seconds"),
          Effect.result
        ), [peer])
      const answered = result._tag === "Success" && result.success._tag === "Some"
      expect(answered).toBe(false)
    })))
})
