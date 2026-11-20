/**
 * A chat room browsers can join through any node. Browsers can't run iroh (the JS package is
 * Node-only), so each node serves the room to browsers over a WebSocket and reaches the room's
 * host over iroh. Both hops speak the same RoomRpcs.
 *
 *   bun run src/main.ts demo                          # host on :4301 + gateway on :4300, one process
 *   bun run src/main.ts host [--port 4301]             # prints a ticket (n0 relays + address lookup)
 *   bun run src/main.ts gateway <ticket> [--port 4300]
 *
 * Open both ports in a browser: messages posted on one appear on the other.
 */
import { BunHttpServer } from "@effect/platform-bun"
import { Effect, Layer, PubSub, Ref, Schema, Stream } from "effect"
import { HttpRouter, HttpServerResponse } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import type { Rpc, RpcGroup } from "effect/rpc"
import { IrohEndpoint, IrohRpc, IrohTicket } from "effect-iroh"
import { RoomRpcs } from "./RoomRpcs.js"
import type { Message } from "./RoomRpcs.js"

type RoomHandlers = Rpc.ToHandler<RpcGroup.Rpcs<typeof RoomRpcs>>

const ALPN = "example/room/1"
const HISTORY = 50

// ---------- the room itself, on the host ----------

const RoomLive = RoomRpcs.toLayer(Effect.gen(function*() {
  const messages = yield* PubSub.unbounded<Message>()
  const history = yield* Ref.make<ReadonlyArray<Message>>([])
  return RoomRpcs.of({
    Join: () =>
      Stream.unwrap(Effect.gen(function*() {
        const subscription = yield* PubSub.subscribe(messages) // before reading history: no gap
        const past = yield* Ref.get(history)
        return Stream.concat(Stream.fromIterable(past), Stream.fromSubscription(subscription))
      })),
    Post: ({ from, text, via }) =>
      Effect.gen(function*() {
        const message = { from, text, via, at: Date.now() }
        yield* Ref.update(history, (h) => [...h, message].slice(-HISTORY))
        yield* PubSub.publish(messages, message)
      })
  })
}))

// ---------- a gateway: the same handlers, forwarded to the host over iroh ----------

const gatewayLive = (host: IrohEndpoint.PeerAddress) =>
  RoomRpcs.toLayer(Effect.gen(function*() {
    const clients = yield* IrohRpc.makePeerClients(RoomRpcs, { alpn: ALPN, resolve: () => Effect.succeed(host) })
    const room = yield* clients(host.id)
    return RoomRpcs.of({
      Join: () => room.Join().pipe(Stream.orDie),
      Post: (message) => room.Post(message).pipe(Effect.orDie)
    })
  })).pipe(Layer.provide(RpcSerialization.layerNdjson))

// ---------- serving a room to browsers ----------

const page = (node: string) => `<!doctype html>
<meta charset="utf-8"><title>room · node ${node}</title>
<style>
  body { font: 15px system-ui, sans-serif; max-width: 40rem; margin: 2rem auto; }
  #log { list-style: none; padding: 0; height: 60vh; overflow-y: auto; border: 1px solid #ccc; }
  #log li { padding: .25rem .5rem; border-bottom: 1px solid #eee; }
  form { display: flex; gap: .5rem; margin-top: .5rem; } #text { flex: 1; }
</style>
<body data-node="${node}">
  <h1>room</h1><p id="me"></p>
  <ul id="log"></ul>
  <form id="form"><input id="text" autocomplete="off" placeholder="say something"><button>send</button></form>
  <script type="module" src="/client.js"></script>
</body>`

const bundleClient = Effect.promise(async () => {
  const build = await Bun.build({ entrypoints: [new URL("./client.ts", import.meta.url).pathname], target: "browser" })
  const [output] = build.outputs
  if (!build.success || output === undefined) throw new Error(build.logs.join("\n"))
  return output.text()
})

/** An HTTP server for browsers: the page, its script, and RoomRpcs over a WebSocket at /rpc. */
const web = <E>(node: string, port: number, handlers: Layer.Layer<RoomHandlers, E>) =>
  Layer.unwrap(Effect.gen(function*() {
    const script = yield* bundleClient
    const routes = Layer.mergeAll(
      HttpRouter.add("GET", "/", HttpServerResponse.html(page(node))),
      HttpRouter.add("GET", "/client.js", HttpServerResponse.text(script, { contentType: "text/javascript" })),
      RpcServer.layerHttp({ group: RoomRpcs, path: "/rpc", protocol: "websocket" }).pipe(
        Layer.provide(handlers),
        Layer.provide(RpcSerialization.layerJson)
      )
    )
    yield* Effect.log(`node ${node}: http://localhost:${port}`)
    return HttpRouter.serve(routes, { disableLogger: true }).pipe(Layer.provide(BunHttpServer.layer({ port })))
  }))

// ---------- nodes ----------

/** The room's host: serves RoomRpcs over iroh to gateways and over WebSocket to its own browsers. */
const host = (port: number) =>
  Effect.gen(function*() {
    const ep = yield* IrohEndpoint.IrohEndpoint
    const room = Layer.succeedContext(yield* Layer.build(RoomLive)) // one room for both transports
    yield* Layer.build(RpcServer.layer(RoomRpcs).pipe(
      Layer.provide(room),
      Layer.provide(IrohRpc.layerProtocolServer({ alpn: ALPN })),
      Layer.provide(RpcSerialization.layerNdjson),
      Layer.provide(Layer.succeed(IrohEndpoint.IrohEndpoint)(ep))
    ))
    yield* Layer.build(web("host", port, room))
    yield* ep.online
    return yield* ep.address
  })

const gateway = (hostAddress: IrohEndpoint.PeerAddress, port: number) =>
  Effect.gen(function*() {
    const ep = Layer.succeed(IrohEndpoint.IrohEndpoint)(yield* IrohEndpoint.IrohEndpoint)
    yield* Layer.build(web("gateway", port, gatewayLive(hostAddress).pipe(Layer.provide(ep))))
  })

// ---------- CLI ----------

const [command, ...args] = process.argv.slice(2)
const flag = (name: string, fallback: number) => {
  const i = args.indexOf(`--${name}`)
  return i >= 0 ? Number(args[i + 1]) : fallback
}
const endpoint = (network: IrohEndpoint.Network) => IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey(), network })

const program =
  command === "host"
    ? Effect.gen(function*() {
      const address = yield* host(flag("port", 4301))
      yield* Effect.log(`ticket: ${IrohTicket.encode(address)}`)
      return yield* Effect.never
    }).pipe(Effect.provide(endpoint("n0")))
    : command === "gateway" && args[0] !== undefined
    ? Effect.gen(function*() {
      const hostAddress = yield* Schema.decodeEffect(IrohTicket.PeerAddressFromTicket)(args[0]!)
      yield* gateway(hostAddress, flag("port", 4300))
      return yield* Effect.never
    }).pipe(Effect.provide(endpoint("n0")))
    : command === "demo"
    ? Effect.gen(function*() {
      // Built into the outer scope so both endpoints live as long as the demo.
      const hostEp = yield* Layer.build(endpoint("private"))
      const address = yield* host(4301).pipe(Effect.provide(hostEp))
      const gatewayEp = yield* Layer.build(endpoint("private"))
      yield* gateway(address, 4300).pipe(Effect.provide(gatewayEp))
      return yield* Effect.never
    })
    : Effect.log("usage: demo | host [--port N] | gateway <ticket> [--port N]")

Effect.runFork(Effect.scoped(program))
