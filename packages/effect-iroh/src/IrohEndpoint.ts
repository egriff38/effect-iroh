/**
 * A bound iroh endpoint as a scoped service, in the shape of `HttpServer` / `SocketServer`:
 * options go to the layer, the service reports what was actually bound, and protocols
 * register per-ALPN accept queues instead of owning the accept loop.
 */
import iroh from "@number0/iroh"
import type * as I from "@number0/iroh"
import { Config, Context, Effect, FiberSet, Layer, Queue, Redacted } from "effect"
import type { Scope } from "effect"
import { NetAddress } from "effect/net"

export interface PeerAddress {
  /** The peer's EndpointId (hex ed25519 public key); this is what iroh authenticates. */
  readonly id: string
  /** Direct addresses to try. Only reachability: identity is checked on the key. */
  readonly addresses: ReadonlyArray<NetAddress.InetAddress>
}

export class IrohEndpoint extends Context.Service<IrohEndpoint, {
  readonly endpoint: I.Endpoint
  /** This endpoint's EndpointId. */
  readonly id: string
  /** Sockets actually bound, port 0 resolved, as `HttpServer.address` reports its own. */
  readonly addresses: ReadonlyArray<NetAddress.InetAddress>
  /**
   * Incoming connections negotiated with `alpn`, after the TLS handshake. Registering advertises
   * the ALPN; closing the scope withdraws it. Connections for unregistered ALPNs are refused.
   */
  readonly accept: (alpn: string) => Effect.Effect<Queue.Dequeue<I.Connection>, never, Scope.Scope>
  /** Dials `peer` over `alpn`; the connection closes with the scope. */
  readonly connect: (peer: PeerAddress, alpn: string) => Effect.Effect<I.Connection, unknown, Scope.Scope>
}>()("effect-iroh/IrohEndpoint") {}

export interface Options {
  /** 32-byte ed25519 secret key, hex. Fixes the EndpointId. */
  readonly secretKey: Redacted.Redacted<string>
  /** Defaults to 127.0.0.1:0. iroh also binds an IPv6 socket alongside it. */
  readonly bind?: NetAddress.InetAddress | undefined
}

const bytes = (s: string) => Array.from(new TextEncoder().encode(s))
const text = (b: ReadonlyArray<number>) => new TextDecoder().decode(Uint8Array.from(b))

/** Private endpoint: no n0 discovery, no relays. Peers are dialed by key plus direct addresses. */
export const make = (options: Options): Effect.Effect<IrohEndpoint["Service"], never, Scope.Scope> =>
  Effect.gen(function*() {
    const bind = options.bind ?? NetAddress.inetAddressUnsafe(NetAddress.ipv4Loopback, 0)
    const endpoint = yield* Effect.acquireRelease(
      Effect.promise(() => {
        const b = iroh.Endpoint.builder()
        b.applyMinimal()
        b.secretKey(Array.from(Buffer.from(Redacted.value(options.secretKey), "hex")))
        b.relayMode(iroh.RelayMode.disabled())
        b.bindAddr(NetAddress.formatInet(bind))
        return b.bind()
      }),
      (ep) => Effect.promise(() => ep.close())
    )

    const routes = new Map<string, Queue.Queue<I.Connection>>()
    const advertise = () => endpoint.setAlpns([...routes.keys()].map(bytes))

    const handshakes = yield* FiberSet.make()
    yield* Effect.gen(function*() {
      while (true) {
        const incoming = yield* Effect.promise(() => endpoint.acceptNext())
        if (incoming === null) return
        yield* FiberSet.run(
          handshakes,
          Effect.tryPromise(async () => (await incoming.accept()).connect()).pipe(
            Effect.flatMap((conn) => {
              const queue = routes.get(text(conn.alpn()))
              return queue
                ? Queue.offer(queue, conn)
                : Effect.sync(() => conn.close(0n, bytes("unknown alpn")))
            }),
            Effect.catchCause((cause) => Effect.logDebug("iroh handshake failed", cause))
          )
        )
      }
    }).pipe(Effect.forkScoped)

    return IrohEndpoint.of({
      endpoint,
      id: endpoint.id().toString(),
      addresses: endpoint.boundSockets().map(NetAddress.inetAddressFromStringUnsafe),
      accept: (alpn) =>
        Effect.acquireRelease(
          Effect.gen(function*() {
            const queue = yield* Queue.unbounded<I.Connection>()
            routes.set(alpn, queue)
            advertise()
            return queue
          }),
          () => Effect.sync(() => {
            routes.delete(alpn)
            advertise()
          })
        ),
      connect: (peer, alpn) =>
        Effect.acquireRelease(
          Effect.tryPromise(() =>
            endpoint.connect(
              new iroh.EndpointAddr(iroh.EndpointId.fromString(peer.id), null, peer.addresses.map(NetAddress.formatInet)),
              bytes(alpn)
            )
          ),
          (conn) => Effect.sync(() => conn.close(0n, []))
        )
    })
  })

export const layer = (options: Options): Layer.Layer<IrohEndpoint> => Layer.effect(IrohEndpoint, make(options))

/**
 * `layer`, read from Config:
 * `layerConfig({ secretKey: Config.Redacted("IROH_SECRET_KEY"), bind: Config.schema(Schema.InetAddressFromString, "IROH_BIND") })`
 */
export const layerConfig = (options: Config.Wrap<Options>): Layer.Layer<IrohEndpoint, Config.ConfigError> =>
  Layer.unwrap(Effect.gen(function*() {
    return layer(yield* Config.unwrap(options))
  }))

/** A fresh random secret key, hex, for tests and examples. */
export const generateSecretKey = (): Redacted.Redacted<string> =>
  Redacted.make(Buffer.from(iroh.SecretKey.generate().toBytes()).toString("hex"))
