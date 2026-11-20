/**
 * iroh endpoint tickets (`endpoint...` strings: key + relay + direct addresses) as a Schema, so a
 * peer can come from Config, a CLI argument, a QR code or any decoded payload.
 */
import iroh from "@number0/iroh"
import { Effect, Schema, SchemaIssue, SchemaTransformation } from "effect"
import { fromEndpointAddr, toEndpointAddr } from "./IrohEndpoint.js"
import type { PeerAddress } from "./IrohEndpoint.js"

export const PeerAddressSchema = Schema.Struct({
  id: Schema.String,
  addresses: Schema.Array(Schema.InetAddress),
  relay: Schema.optional(Schema.String)
})

/** Encodes a peer as a ticket string. */
export const encode = (peer: PeerAddress): string => iroh.EndpointTicket.fromAddr(toEndpointAddr(peer)).toString()

/** Decodes a ticket string into a `PeerAddress`; encodes back to a ticket. */
export const PeerAddressFromTicket: Schema.Codec<PeerAddress, string> = Schema.String.pipe(
  Schema.decodeTo(
    PeerAddressSchema,
    SchemaTransformation.transformEffect({
      decode: (ticket: string) =>
        Effect.try({
          try: () => fromEndpointAddr(iroh.EndpointTicket.fromString(ticket).endpointAddr()),
          catch: () => new SchemaIssue.InvalidValue({ message: "not an iroh endpoint ticket" }, ticket)
        }),
      encode: (peer: PeerAddress) => Effect.succeed(encode(peer))
    })
  )
) as Schema.Codec<PeerAddress, string>
