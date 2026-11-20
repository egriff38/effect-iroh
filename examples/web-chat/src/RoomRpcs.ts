/** The room contract, shared by browsers (over WebSocket) and nodes (over iroh). */
import { Schema } from "effect"
import { Rpc, RpcGroup } from "effect/rpc"

export const Message = Schema.Struct({
  from: Schema.String,
  text: Schema.String,
  at: Schema.Number,
  /** Which node the author's browser was connected to. */
  via: Schema.String
})
export type Message = typeof Message.Type

export class RoomRpcs extends RpcGroup.make(
  /** Recent history, then every new message. */
  Rpc.make("Join", { success: Message, stream: true }),
  Rpc.make("Post", { payload: { from: Schema.String, text: Schema.String, via: Schema.String } })
) {}
