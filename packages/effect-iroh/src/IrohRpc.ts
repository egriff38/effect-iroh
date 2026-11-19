/** Effect RPC client and server protocols over iroh, plus a per-peer client cache. */
import { Effect, Layer, RcMap } from "effect"
import type { Duration } from "effect"
import type { Scope } from "effect"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"
import type { Rpc, RpcClientError, RpcGroup } from "effect/rpc"
import { Socket } from "effect/socket"
import type { IrohEndpoint, PeerAddress } from "./IrohEndpoint.js"
import { layerSocketServer, makeSocket } from "./IrohSocket.js"
import type { ServerOptions } from "./IrohSocket.js"

export interface ClientOptions {
  /** Passed to `RpcClient.makeProtocolSocket`. Default 5 seconds. */
  readonly pingInterval?: Duration.Input | undefined
  /** Time without any server frame before in-flight calls fail. Defaults to `pingInterval`. */
  readonly pingTimeout?: Duration.Input | undefined
}

/**
 * Client protocol to one peer. `retryTransientErrors` stays off so connection failures reach
 * callers as `RpcClientError` instead of being retried silently; streams that care resubscribe.
 */
export const makeProtocolClient = (
  peer: PeerAddress,
  alpn: string,
  options?: ClientOptions
): Effect.Effect<RpcClient.Protocol["Service"], never, IrohEndpoint | RpcSerialization.RpcSerialization | Scope.Scope> =>
  Effect.flatMap(makeSocket(peer, alpn), (socket) =>
    RpcClient.makeProtocolSocket({ ...options, retryTransientErrors: false }).pipe(
      Effect.provideService(Socket.Socket, socket)
    ))

export const layerProtocolClient = (
  peer: PeerAddress,
  alpn: string,
  options?: ClientOptions
): Layer.Layer<RpcClient.Protocol, never, IrohEndpoint | RpcSerialization.RpcSerialization> =>
  Layer.effect(RpcClient.Protocol, makeProtocolClient(peer, alpn, options))

/** Server protocol for `RpcServer.layer`, accepting `alpn` on the context's IrohEndpoint. */
export const layerProtocolServer = (
  options: ServerOptions
): Layer.Layer<RpcServer.Protocol, never, IrohEndpoint | RpcSerialization.RpcSerialization> =>
  RpcServer.layerProtocolSocketServer.pipe(Layer.provide(layerSocketServer(options)))

export interface PeerClientsOptions extends ClientOptions {
  readonly alpn: string
  /** Where a peer can be reached right now (static table, directory, discovery). */
  readonly resolve: (peer: string) => Effect.Effect<PeerAddress>
  readonly idleTimeToLive?: `${number} seconds` | undefined
}

/**
 * Typed clients keyed by EndpointId, one connection per peer, shared while in use and closed
 * after `idleTimeToLive` (default 30 seconds).
 */
export const makePeerClients = <Rpcs extends Rpc.Any>(
  group: RpcGroup.RpcGroup<Rpcs>,
  options: PeerClientsOptions
): Effect.Effect<
  (peer: string) => Effect.Effect<RpcClient.RpcClient<Rpcs, RpcClientError.RpcClientError>, never, Scope.Scope>,
  never,
  IrohEndpoint | RpcSerialization.RpcSerialization | Rpc.MiddlewareClient<Rpcs> | Scope.Scope
> =>
  Effect.gen(function*() {
    const clients = yield* RcMap.make({
      lookup: (peer: string) =>
        Effect.gen(function*() {
          const protocol = yield* makeProtocolClient(yield* options.resolve(peer), options.alpn, options)
          return yield* Effect.provideService(RpcClient.make(group), RpcClient.Protocol, protocol)
        }),
      idleTimeToLive: options.idleTimeToLive ?? "30 seconds"
    })
    return (peer: string) => RcMap.get(clients, peer)
  })
