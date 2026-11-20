/**
 * Pair two machines with a ticket, wherever they are, and watch the connection's health live.
 *
 *   bun run src/main.ts host                      # prints a ticket, serves until Ctrl-C
 *   bun run src/main.ts join <ticket> [--relay-first]
 *   bun run src/main.ts demo                      # both in one process, relay first
 *
 * Uses n0's public relays and address lookup (`network: "n0"`), so it needs internet access.
 * `--relay-first` drops the ticket's direct addresses: the dial goes through the relay and iroh
 * then hole-punches to a direct path, which the health atom shows as it happens.
 */
import { Effect, Layer, Schema } from "effect"
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity"
import { Rpc, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import { IrohConnection, IrohEndpoint, IrohRpc, IrohTicket } from "effect-iroh"

class PairRpcs extends RpcGroup.make(
  Rpc.make("Hello", { payload: { from: Schema.String }, success: Schema.String })
) {}

const ALPN = "example/pair/1"
const endpoint = () => IrohEndpoint.layer({ secretKey: IrohEndpoint.generateSecretKey(), network: "n0" })

const host = Effect.gen(function*() {
  const ep = yield* IrohEndpoint.IrohEndpoint
  yield* Layer.build(RpcServer.layer(PairRpcs).pipe(
    Layer.provide(PairRpcs.toLayer({ Hello: ({ from }) => Effect.succeed(`hello ${from.slice(0, 10)}, from ${ep.id.slice(0, 10)}`) })),
    Layer.provide(IrohRpc.layerProtocolServer({ alpn: ALPN })),
    Layer.provide(RpcSerialization.layerNdjson),
    Layer.provide(Layer.succeed(IrohEndpoint.IrohEndpoint)(ep))
  ))
  yield* ep.online
  return IrohTicket.encode(yield* ep.address)
})

const join = (ticket: string, options: { readonly relayFirst: boolean; readonly watchFor: number }) =>
  Effect.gen(function*() {
    const ep = yield* IrohEndpoint.IrohEndpoint
    const decoded = yield* Schema.decodeEffect(IrohTicket.PeerAddressFromTicket)(ticket)
    const peer = options.relayFirst ? { ...decoded, addresses: [] } : decoded
    yield* ep.online

    // Connection health as an ordinary atom over the endpoint's stream; watched before dialing so
    // the relay -> direct upgrade is visible.
    const started = Date.now()
    const runtime = Atom.runtime(Layer.succeed(IrohEndpoint.IrohEndpoint)(ep))
    const health = runtime.atom(IrohConnection.healthOf(peer.id))
    const registry = AtomRegistry.make()
    let last = ""
    const unsubscribe = registry.subscribe(health, (h) => {
      if (!AsyncResult.isSuccess(h) || h.value === undefined) return
      const paths = h.value.paths.map((p) => `${p.selected ? "*" : " "}${p.kind} ${p.remote}`).join(" | ")
      const line = `via ${h.value.via ?? "(none)"}   ${paths}`
      if (line === last) return
      last = line
      console.log(`+${String(Date.now() - started).padStart(5)} ms  rtt ${h.value.rttMs ?? "?"} ms  ${line}`)
    }, { immediate: true })

    const clients = yield* IrohRpc.makePeerClients(PairRpcs, { alpn: ALPN, resolve: () => Effect.succeed(peer) })
    const client = yield* clients(peer.id)
    console.log(`${yield* client.Hello({ from: ep.id })}  (${Date.now() - started} ms)`)
    yield* Effect.sleep(`${options.watchFor} seconds`)
    unsubscribe()
  }).pipe(Effect.provide(RpcSerialization.layerNdjson))

const [command, ...args] = process.argv.slice(2)
const relayFirst = args.includes("--relay-first")

const program =
  command === "host"
    ? Effect.gen(function*() {
      console.log(`ticket: ${yield* host}`)
      return yield* Effect.never
    }).pipe(Effect.provide(endpoint()))
    : command === "join" && args[0] !== undefined
    ? join(args[0], { relayFirst, watchFor: 15 }).pipe(Effect.provide(endpoint()))
    : command === "demo"
    ? Effect.gen(function*() {
      // Built into the outer scope: Effect.provide(layer) would close the host's endpoint on return.
      const hostEndpoint = yield* Layer.build(endpoint())
      const ticket = yield* host.pipe(Effect.provide(hostEndpoint))
      console.log(`host ticket: ${ticket.slice(0, 40)}…`)
      yield* join(ticket, { relayFirst: true, watchFor: 6 }).pipe(Effect.provide(endpoint()))
    })
    : Effect.sync(() => console.log("usage: host | join <ticket> [--relay-first] | demo"))

await Effect.runPromise(Effect.scoped(program))
process.exit(0)
