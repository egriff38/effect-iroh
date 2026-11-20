// @env browser
/** Browser side: the same RoomRpcs, over a WebSocket to whichever node served this page. */
import { Effect, Layer, Stream } from "effect"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { Socket } from "effect/socket"
import { RoomRpcs } from "./RoomRpcs.js"

const node = document.body.dataset["node"] ?? "?"
const log = document.querySelector<HTMLUListElement>("#log")!
const form = document.querySelector<HTMLFormElement>("#form")!
const input = document.querySelector<HTMLInputElement>("#text")!
const name = `guest-${Math.random().toString(36).slice(2, 6)}`
document.querySelector("#me")!.textContent = `${name} on node ${node}`

const ProtocolLive = RpcClient.layerProtocolSocket().pipe(
  Layer.provide(Socket.layerWebSocket(`ws://${location.host}/rpc`)),
  Layer.provide(Socket.layerWebSocketConstructorGlobal),
  Layer.provide(RpcSerialization.layerJson)
)

const program = Effect.gen(function*() {
  const client = yield* RpcClient.make(RoomRpcs)
  form.addEventListener("submit", (event) => {
    event.preventDefault()
    const text = input.value.trim()
    if (text === "") return
    input.value = ""
    Effect.runFork(client.Post({ from: name, text, via: node }))
  })
  yield* Stream.runForEach(client.Join(), (m) =>
    Effect.sync(() => {
      const li = document.createElement("li")
      li.textContent = `${new Date(m.at).toLocaleTimeString()}  ${m.from} (via ${m.via}): ${m.text}`
      log.append(li)
      li.scrollIntoView()
    }))
}).pipe(Effect.scoped, Effect.provide(ProtocolLive))

Effect.runFork(program)
