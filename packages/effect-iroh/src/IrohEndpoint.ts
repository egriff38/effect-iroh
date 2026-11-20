/**
 * A bound iroh endpoint as a scoped service, in the shape of `HttpServer` / `SocketServer`:
 * options go to the layer, the service reports what was actually bound, and protocols
 * register per-ALPN accept queues instead of owning the accept loop.
 */
import iroh from "@number0/iroh"
import type * as I from "@number0/iroh"
import { Config, Context, Effect, FiberSet, Layer, Queue, Redacted, Result, Scope, SubscriptionRef } from "effect"
import { NetAddress } from "effect/net"

export interface PeerAddress {
  /** The peer's EndpointId (hex ed25519 public key); this is what iroh authenticates. */
  readonly id: string
  /** Direct addresses to try. Only reachability: identity is checked on the key. */
  readonly addresses: ReadonlyArray<NetAddress.InetAddress>
  /** Home relay URL, if the peer has one; lets the dial work before any direct path exists. */
  readonly relay?: string | undefined
}

export class IrohEndpoint extends Context.Service<IrohEndpoint, {
  readonly endpoint: I.Endpoint
  /** This endpoint's EndpointId. */
  readonly id: string
  /** Sockets actually bound, port 0 resolved, as `HttpServer.address` reports its own. */
  readonly addresses: ReadonlyArray<NetAddress.InetAddress>
  /** Waits until the endpoint has a home relay (immediate for `network: "private"`). */
  readonly online: Effect.Effect<void>
  /** How peers can reach this endpoint right now: direct addresses and home relay. */
  readonly address: Effect.Effect<PeerAddress>
  /** Open connections by remote EndpointId, dialed or accepted; closed ones drop out. */
  readonly connections: SubscriptionRef.SubscriptionRef<ReadonlyMap<string, I.Connection>>
  /**
   * Incoming connections negotiated with `alpn`, after the TLS handshake. Registering advertises
   * the ALPN; closing the scope withdraws it. Connections for unregistered ALPNs are refused.
   */
  readonly accept: (alpn: string) => Effect.Effect<Queue.Dequeue<I.Connection>, never, Scope.Scope>
  /** Dials `peer` over `alpn`; the connection closes with the scope. */
  readonly connect: (peer: PeerAddress, alpn: string) => Effect.Effect<I.Connection, unknown, Scope.Scope>
}>()("effect-iroh/IrohEndpoint") {}

/**
 * - `"private"`: no relays, no discovery; peers are dialed by key plus direct addresses.
 * - `"n0"`: n0's public relays and DNS address lookup, so peers anywhere can dial by ticket.
 * - `{ relays }`: n0 address lookup with your own relays.
 */
export type Network = "private" | "n0" | { readonly relays: ReadonlyArray<string> }

export interface Options {
  /** 32-byte ed25519 secret key, hex. Fixes the EndpointId. */
  readonly secretKey: Redacted.Redacted<string>
  /** Defaults to `"private"`. */
  readonly network?: Network | undefined
  /**
   * Defaults to 127.0.0.1:0 for `"private"` and to iroh's own (all interfaces, ephemeral port)
   * otherwise. iroh also binds an IPv6 socket alongside an IPv4 one.
   */
  readonly bind?: NetAddress.InetAddress | undefined
}

const bytes = (s: string) => Array.from(new TextEncoder().encode(s))
const text = (b: ReadonlyArray<number>) => new TextDecoder().decode(Uint8Array.from(b))

/** Parses iroh's `ip:port` strings, skipping any it reports that NetAddress doesn't model. */
export const parseAddresses = (raw: ReadonlyArray<string>): ReadonlyArray<NetAddress.InetAddress> =>
  raw.flatMap((s) => {
    const parsed = NetAddress.inetAddressFromString(s)
    return Result.isSuccess(parsed) ? [parsed.success] : []
  })

export const toEndpointAddr = (peer: PeerAddress): I.EndpointAddr =>
  new iroh.EndpointAddr(iroh.EndpointId.fromString(peer.id), peer.relay ?? null, peer.addresses.map(NetAddress.formatInet))

export const fromEndpointAddr = (addr: I.EndpointAddr): PeerAddress => ({
  id: addr.id().toString(),
  addresses: parseAddresses(addr.directAddresses()),
  relay: addr.relayUrl() ?? undefined
})

export const make = (options: Options): Effect.Effect<IrohEndpoint["Service"], never, Scope.Scope> =>
  Effect.gen(function*() {
    const network = options.network ?? "private"
    const bind = options.bind ?? (network === "private" ? NetAddress.inetAddressUnsafe(NetAddress.ipv4Loopback, 0) : undefined)
    const endpoint = yield* Effect.acquireRelease(
      Effect.promise(() => {
        const b = iroh.Endpoint.builder()
        if (network === "private") {
          b.applyMinimal()
          b.relayMode(iroh.RelayMode.disabled())
        } else {
          b.applyN0()
          if (network !== "n0") b.relayMode(iroh.RelayMode.customFromUrls([...network.relays]))
        }
        b.secretKey(Array.from(Buffer.from(Redacted.value(options.secretKey), "hex")))
        if (bind) b.bindAddr(NetAddress.formatInet(bind))
        return b.bind()
      }),
      (ep) => Effect.promise(() => ep.close())
    )

    const connections = yield* SubscriptionRef.make<ReadonlyMap<string, I.Connection>>(new Map())
    const track = (conn: I.Connection) =>
      Effect.gen(function*() {
        const id = conn.remoteId().toString()
        yield* SubscriptionRef.update(connections, (m) => new Map(m).set(id, conn))
        yield* Effect.forkScoped(Effect.promise(() => conn.closed().catch(() => "")).pipe(
          Effect.andThen(SubscriptionRef.update(connections, (m) => {
            if (m.get(id) !== conn) return m
            const next = new Map(m)
            next.delete(id)
            return next
          }))
        ))
      })

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
                ? Effect.andThen(track(conn), Queue.offer(queue, conn))
                : Effect.sync(() => conn.close(0n, bytes("unknown alpn")))
            }),
            Effect.catchCause((cause) => Effect.logDebug("iroh handshake failed", cause))
          )
        )
      }
    }).pipe(Effect.forkScoped)

    // Tracking forks live as long as the endpoint, not the caller's scope.
    const endpointScope = yield* Effect.scope
    return IrohEndpoint.of({
      endpoint,
      id: endpoint.id().toString(),
      addresses: parseAddresses(endpoint.boundSockets()),
      online: network === "private" ? Effect.void : Effect.promise(() => endpoint.online()),
      address: Effect.sync(() => fromEndpointAddr(endpoint.addr())),
      connections,
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
          Effect.tryPromise(() => endpoint.connect(toEndpointAddr(peer), bytes(alpn))).pipe(
            Effect.tap((conn) => track(conn).pipe(Effect.provideService(Scope.Scope, endpointScope)))
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
