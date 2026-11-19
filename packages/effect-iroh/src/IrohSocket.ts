/**
 * iroh bidirectional streams as Effect `Socket`s, so every socket-based Effect protocol
 * (RPC, the socket server) runs over iroh unchanged.
 */
import type * as I from "@number0/iroh"
import { Effect, FiberSet, Layer, Queue } from "effect"
import { Socket, SocketServer } from "effect/socket"
import { IrohEndpoint } from "./IrohEndpoint.js"
import type { PeerAddress } from "./IrohEndpoint.js"

const READ_CHUNK = 16 * 1024

// The napi bindings move bytes as Array<number>: one copy each direction per chunk.
const toTransform = (bi: I.BiStream): Socket.InputTransformStream => ({
  readable: new ReadableStream<Uint8Array>({
    async pull(controller) {
      const chunk = await bi.recv.read(READ_CHUNK)
      if (chunk.length === 0) controller.close()
      else controller.enqueue(Uint8Array.from(chunk))
    },
    // Not awaited: RecvStream.stop takes the same lock as a pending read(), so awaiting it
    // deadlocks Socket's reader.cancel() finalizer against a dead peer. Closing the connection
    // aborts the read.
    cancel() {
      bi.recv.stop(0n).catch(() => {})
    }
  }),
  writable: new WritableStream<Uint8Array>({
    write: (chunk) => bi.send.writeAll(Array.from(chunk)),
    close: () => bi.send.finish()
  })
})

const openError = (cause: unknown) =>
  new Socket.SocketError({ reason: new Socket.SocketOpenError({ kind: "Unknown", cause }) })

/** A client Socket to `peer`; each run (and so each reconnect) dials a fresh connection. */
export const makeSocket = (peer: PeerAddress, alpn: string): Effect.Effect<Socket.Socket, never, IrohEndpoint> =>
  Effect.gen(function*() {
    const ep = yield* IrohEndpoint
    return yield* Socket.fromTransformStream(
      ep.connect(peer, alpn).pipe(
        Effect.flatMap((conn) => Effect.tryPromise(() => conn.openBi())),
        Effect.map(toTransform),
        Effect.mapError(openError)
      )
    )
  })

export interface ServerOptions {
  readonly alpn: string
  /**
   * Membership check on the TLS-authenticated remote EndpointId. Defaults to allowing every
   * peer that can reach the endpoint.
   */
  readonly allow?: ((peer: string) => boolean) | undefined
}

/** SocketServer accepting `alpn` connections on the context's IrohEndpoint. One stream per connection. */
export const layerSocketServer = (options: ServerOptions): Layer.Layer<SocketServer.SocketServer, never, IrohEndpoint> =>
  Layer.effect(SocketServer.SocketServer, Effect.gen(function*() {
    const ep = yield* IrohEndpoint
    const [address] = ep.addresses
    if (address === undefined) return yield* Effect.die("iroh endpoint bound no sockets")
    const allow = options.allow ?? (() => true)
    return SocketServer.SocketServer.of({
      address,
      run: (handler) =>
        Effect.gen(function*() {
          const connections = yield* ep.accept(options.alpn)
          const fibers = yield* FiberSet.make()
          while (true) {
            const conn = yield* Queue.take(connections)
            yield* FiberSet.run(
              fibers,
              Effect.gen(function*() {
                const remote = conn.remoteId().toString()
                if (!allow(remote)) {
                  yield* Effect.logWarning(`effect-iroh: refused ${remote.slice(0, 10)}`)
                  conn.close(403n, Array.from(new TextEncoder().encode("not a member")))
                  return
                }
                const bi = yield* Effect.tryPromise(() => conn.acceptBi())
                const socket = yield* Socket.fromTransformStream(Effect.succeed(toTransform(bi)))
                yield* handler(socket)
              }).pipe(
                Effect.ensuring(Effect.sync(() => conn.close(0n, []))),
                Effect.catchCause((cause) => Effect.logDebug("effect-iroh: connection ended", cause))
              )
            )
          }
        }).pipe(Effect.scoped)
    })
  }))
