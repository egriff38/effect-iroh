import { describe, expect, test } from "bun:test"
import { Context, Deferred, Effect, Layer, Schema, Stream } from "effect"
import type { Scope } from "effect"
import { NetAddress } from "effect/net"
import { Rpc, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import type { RpcClient, RpcClientError } from "effect/rpc"
import { IrohConnection, IrohEndpoint, IrohRpc, IrohTicket } from "../src/index.js"

class Echo extends RpcGroup.make(
  Rpc.make("Echo", { payload: { text: Schema.String }, success: Schema.String }),
  Rpc.make("Count", { payload: { to: Schema.Number }, success: Schema.Number, stream: true }),
  Rpc.make("Forever", { success: Schema.Number, stream: true })
) {}

const ALPN = "effect-iroh/test/0"

type EchoClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof Echo>, RpcClientError.RpcClientError>
type Clients = (peer: string) => Effect.Effect<EchoClient, never, Scope.Scope>
type Endpoint = IrohEndpoint.IrohEndpoint["Service"]

/** A bound endpoint plus an Echo server on it, alive for the test's scope. */
const server = (allow?: (peer: string) => boolean) =>
  Effect.gen(function*() {
    const foreverEnded = yield* Deferred.make<void>()
    const handlers = Echo.toLayer({
      Echo: ({ text }) => Effect.succeed(text),
      Count: ({ to }) => Stream.range(1, to),
      Forever: () => Stream.concat(Stream.make(1), Stream.never).pipe(Stream.ensuring(Deferred.succeed(foreverEnded, undefined)))
    })
    const ctx = yield* Layer.build(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() }))
    yield* Layer.build(
      RpcServer.layer(Echo).pipe(
        Layer.provide(handlers),
        Layer.provide(IrohRpc.layerProtocolServer({ alpn: ALPN, allow })),
        Layer.provide(RpcSerialization.layerNdjson),
        Layer.provide(Layer.succeedContext(ctx))
      )
    )
    return { endpoint: Context.get(ctx, IrohEndpoint.IrohEndpoint), foreverEnded }
  })

const peerOf = (ep: Endpoint): IrohEndpoint.PeerAddress => ({ id: ep.id, addresses: ep.addresses })

const client = <A, E>(
  f: (clients: Clients, self: Endpoint) => Effect.Effect<A, E, Scope.Scope | IrohEndpoint.IrohEndpoint>,
  peers: ReadonlyArray<IrohEndpoint.PeerAddress>,
  streams?: IrohRpc.ClientOptions["streams"]
) =>
  Effect.gen(function*() {
    const clients = yield* IrohRpc.makePeerClients(Echo, {
      alpn: ALPN,
      streams,
      resolve: (id) => Effect.succeed(peers.find((p) => p.id === id) ?? { id, addresses: [] })
    })
    return yield* f(clients, yield* IrohEndpoint.IrohEndpoint)
  }).pipe(
    Effect.provide(IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey() })),
    Effect.provide(RpcSerialization.layerNdjson)
  )

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) => Effect.runPromise(Effect.scoped(effect))

describe("IrohEndpoint", () => {
  test("binds an ephemeral port and reports the resolved address", () =>
    run(Effect.gen(function*() {
      const { endpoint } = yield* server()
      const v4 = endpoint.addresses.find(NetAddress.isInetAddressV4)
      expect(v4?.port).toBeGreaterThan(0)
      expect(endpoint.id).toMatch(/^[0-9a-f]{64}$/)
    })))
})

describe("IrohTicket", () => {
  test("a ticket from one endpoint dials it", () =>
    run(Effect.gen(function*() {
      const { endpoint } = yield* server()
      const ticket = IrohTicket.encode(yield* endpoint.address)
      const peer = yield* Schema.decodeEffect(IrohTicket.PeerAddressFromTicket)(ticket)
      expect(peer.id).toBe(endpoint.id)
      const echoed = yield* client((clients) => Effect.flatMap(clients(peer.id), (e) => e.Echo({ text: "via ticket" })), [peer])
      expect(echoed).toBe("via ticket")
    })))

  test("a string that is not a ticket fails to decode", () =>
    run(Effect.gen(function*() {
      const result = yield* Schema.decodeEffect(IrohTicket.PeerAddressFromTicket)("nope").pipe(Effect.result)
      expect(result._tag).toBe("Failure")
    })))
})

describe.each(["per-request", "shared"] as const)("IrohRpc (%s streams)", (streams) => {
  test("unary and streaming calls round-trip over iroh", () =>
    run(Effect.gen(function*() {
      const { endpoint } = yield* server()
      const [echoed, counted] = yield* client((clients) =>
        Effect.gen(function*() {
          const echo = yield* clients(endpoint.id)
          return yield* Effect.all([
            echo.Echo({ text: "hello" }),
            Stream.runCollect(echo.Count({ to: 3 }))
          ], { concurrency: "unbounded" })
        }), [peerOf(endpoint)], streams)
      expect(echoed).toBe("hello")
      expect(counted).toEqual([1, 2, 3])
    })))

  test("interrupting a streaming call ends it on the server", () =>
    run(Effect.gen(function*() {
      const { endpoint, foreverEnded } = yield* server()
      yield* client((clients) =>
        Effect.flatMap(clients(endpoint.id), (echo) => Stream.runHead(echo.Forever())), [peerOf(endpoint)], streams)
      const ended = yield* Deferred.await(foreverEnded).pipe(Effect.timeoutOption("2 seconds"))
      expect(ended._tag).toBe("Some")
    })))

  test("a peer outside `allow` gets no response", () =>
    run(Effect.gen(function*() {
      const { endpoint } = yield* server(() => false)
      const result = yield* client((clients) =>
        Effect.flatMap(clients(endpoint.id), (echo) => echo.Echo({ text: "let me in" })).pipe(
          Effect.timeoutOption("2 seconds"),
          Effect.result
        ), [peerOf(endpoint)], streams)
      const answered = result._tag === "Success" && result.success._tag === "Some"
      expect(answered).toBe(false)
    })))
})

describe("IrohConnection", () => {
  test("health of a live connection reports a selected direct path", () =>
    run(Effect.gen(function*() {
      const { endpoint } = yield* server()
      const health = yield* client((clients) =>
        Effect.gen(function*() {
          yield* Effect.flatMap(clients(endpoint.id), (e) => e.Echo({ text: "warm up" }))
          return yield* IrohConnection.healthOf(endpoint.id).pipe(
            Stream.filter((h) => h?.via !== undefined),
            Stream.runHead
          )
        }), [peerOf(endpoint)])
      expect(health._tag).toBe("Some")
      if (health._tag === "Some") {
        expect(health.value?.via).toBe("direct")
        expect(health.value?.rttMs).toBeGreaterThanOrEqual(0)
      }
    })))
})
