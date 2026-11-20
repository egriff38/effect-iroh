/** Effect RPC client and server protocols over iroh, plus a per-peer client cache. */
import type * as I from "@number0/iroh"
import { Effect, FiberSet, Layer, RcMap, Scope, Semaphore } from "effect"
import type { Duration } from "effect"
import { RpcClient, RpcClientError, RpcSerialization, RpcServer } from "effect/rpc"
import type { Rpc, RpcGroup, RpcMessage } from "effect/rpc"
import { Socket } from "effect/socket"
import { IrohEndpoint } from "./IrohEndpoint.js"
import type { PeerAddress } from "./IrohEndpoint.js"
import { layerSocketServer, makeSocket } from "./IrohSocket.js"
import type { ServerOptions } from "./IrohSocket.js"

export interface ClientOptions {
  /**
   * - `"per-request"` (default): every RPC call gets its own QUIC stream on one connection, so a
   *   large or slow response never delays another call. Liveness is QUIC's own.
   * - `"shared"`: one stream carries every call, framed like a WebSocket, with RPC pings.
   */
  readonly streams?: "per-request" | "shared" | undefined
  /** `"shared"` only: passed to `RpcClient.makeProtocolSocket`. Default 5 seconds. */
  readonly pingInterval?: Duration.Input | undefined
  /** `"shared"` only: time without any server frame before in-flight calls fail. */
  readonly pingTimeout?: Duration.Input | undefined
}

const READ_CHUNK = 64 * 1024
const encoder = new TextEncoder()
const toBytes = (data: string | Uint8Array) => Array.from(typeof data === "string" ? encoder.encode(data) : data)

const streamError = (cause: unknown) =>
  new RpcClientError.RpcClientError({ reason: new Socket.SocketReadError({ cause }) })

/**
 * One QUIC stream per request. The request's acks and interrupts go on its stream; its chunks
 * and exit come back on it. The write side stays open until the exit, because the server ends a
 * request whose stream closes.
 */
const makeProtocolClientPerRequest = (
  peer: PeerAddress,
  alpn: string
): Effect.Effect<RpcClient.Protocol["Service"], never, IrohEndpoint | RpcSerialization.RpcSerialization | Scope.Scope> =>
  RpcClient.Protocol.make(Effect.fnUntraced(function*(writeResponse) {
    const ep = yield* IrohEndpoint
    const serialization = yield* RpcSerialization.RpcSerialization
    const scope = yield* Effect.scope
    const readers = yield* FiberSet.make()
    const dialing = yield* Semaphore.make(1)

    let current: I.Connection | undefined
    const connection = dialing.withPermits(1)(Effect.suspend(() =>
      current !== undefined && current.closeReason() === null
        ? Effect.succeed(current)
        : ep.connect(peer, alpn).pipe(
          Scope.provide(scope),
          Effect.tap((conn) => Effect.sync(() => (current = conn)))
        )
    ))

    const writers = new Map<string, (message: RpcMessage.FromClientEncoded) => Effect.Effect<void, unknown>>()

    const open = (clientId: number, request: RpcMessage.RequestEncoded) =>
      Effect.gen(function*() {
        const requestId = String(request.id)
        const conn = yield* connection
        const bi = yield* Effect.tryPromise(() => conn.openBi())
        const parser = serialization.makeUnsafe()
        const write = (message: RpcMessage.FromClientEncoded) => {
          const encoded = parser.encode(message)
          return encoded === undefined ? Effect.void : Effect.tryPromise(() => bi.send.writeAll(toBytes(encoded)))
        }
        writers.set(requestId, write)
        yield* write(request)
        yield* FiberSet.run(readers, Effect.gen(function*() {
          while (true) {
            const chunk = yield* Effect.tryPromise(() => bi.recv.read(READ_CHUNK))
            if (chunk.length === 0) return yield* Effect.fail(new Error("stream ended before the response"))
            for (const response of parser.decode(Uint8Array.from(chunk)) as Array<RpcMessage.FromServerEncoded>) {
              yield* writeResponse(clientId, response)
              if (response._tag === "Exit") return
            }
          }
        }).pipe(
          Effect.catchCause((cause) =>
            writeResponse(clientId, { _tag: "ClientProtocolError", error: streamError(cause), requestIds: [request.id] })
          ),
          Effect.ensuring(Effect.sync(() => {
            writers.delete(requestId)
            bi.send.finish().catch(() => {})
          }))
        ))
      })

    return {
      send: (clientId, message) => {
        switch (message._tag) {
          case "Request":
            return Effect.mapError(open(clientId, message), streamError)
          case "Ack":
          case "Interrupt": {
            const write = writers.get(String(message.requestId))
            return write ? Effect.mapError(write(message), streamError) : Effect.void
          }
          default:
            return Effect.void // Ping / Eof: QUIC already keeps the connection alive
        }
      },
      supportsAck: true,
      supportsTransferables: false,
      codecFor: serialization.codecFor
    }
  }))

/**
 * Client protocol to one peer. Connection failures reach callers as `RpcClientError` (no silent
 * retries); streams that care resubscribe.
 */
export const makeProtocolClient = (
  peer: PeerAddress,
  alpn: string,
  options?: ClientOptions
): Effect.Effect<RpcClient.Protocol["Service"], never, IrohEndpoint | RpcSerialization.RpcSerialization | Scope.Scope> =>
  options?.streams === "shared"
    ? Effect.flatMap(makeSocket(peer, alpn), (socket) =>
      RpcClient.makeProtocolSocket({
        pingInterval: options.pingInterval,
        pingTimeout: options.pingTimeout,
        retryTransientErrors: false
      }).pipe(Effect.provideService(Socket.Socket, socket)))
    : makeProtocolClientPerRequest(peer, alpn)

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
