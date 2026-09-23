# Norma Residents

This package contains concrete Resident implementations. The shared
`Resident` trait, `ResidentDescriptor`, registration identity, mailbox and
Gate contracts, and RDF/RTDF senders remain in `norma-harness`.

## Codex Resident

This crate adapts a locally authenticated Codex CLI to Norma Harness. It starts
`codex app-server` over stdio, registers a Resident with `codex.agent` and `llm` capabilities, and exchanges
requests and results through RTDF. The Codex child inherits the local CLI's
ChatGPT sign-in; no OpenAI API key is required for subscription sign-in.

The Resident accepts `codex.turn.request` with this JSON payload:

```json
{
  "request_id": "task-1",
  "prompt": "Summarize this repository",
  "thread_id": null
}
```

It sends `codex.turn.result` to the source Resident. The result includes
`request_id`, `thread_id`, `turn_id`, `status`, `final_response`, `error`, and
optional `attachments`.
The Codex Resident keeps a separate provider thread for each chat room.
The web app persists that mapping and restores it after restart. A direct
caller may provide `thread_id` to seed its conversation before its first turn;
after that, the Resident owns continuity and rejects a different thread ID.
An RTDF send result only confirms mailbox delivery; the business result arrives
as a separate message.

## Shared LLM contract and chat coordinator

`llm.rs` defines the `llm` capability and the `llm.turn.request` /
`llm.turn.result` message pair. Requests can also include chronological
`context` events from rooms the Resident has joined. Chat sets `origin` to
`{"kind":"solo"|"group","room_id":7,"room_name":"Design"}`. The LLM
inbound Gate validates that origin and stamps `source_resident` from RTDF's
authenticated sender. Codex sends the model one JSON envelope containing
`source`, `new_events`, and `current_request`. Any Resident advertising
`llm` must use those events as its shared conversation context, answer this
protocol, and return the matching `request_id` to the source Resident. Codex handles this
shared protocol and the original `codex.turn.*` pair.

`chat::ChatResident` owns rooms and correlates replies. A one-member room
sends one turn. A group room sends members turns in sequence, passing previous
contributions to the next member, then asks the first member for a final
synthesis. A group message with `@member` or `@{member}` calls only the named
group members; without mentions, all members collaborate. Chat serializes
turns per Resident and sends the messages from all its rooms in chronological
order. Incognito rooms only receive their own context. The Resident owns
provider thread continuity for each room.

`tools::RoomToolsResident` advertises `tool.room_members`. Codex exposes its
`list_group_members` function through app-server dynamic tools. The function
reads the current group ID from the turn origin, sends a request through RTDF
to the tools Resident, and receives the actual member list from Chat Resident.
Chat allows only a Resident that belongs to that group to query it. Dynamic
tools are currently an experimental Codex app-server protocol.

## Images and GIFs

The web chat accepts up to four PNG, JPEG, WebP, or GIF files per message,
with an 8 MiB limit per file. Messages carry attachment metadata in the
shared LLM context. Codex receives images as `localImage` inputs. GIFs remain
animated in the chat and download as the original file; Codex sees the first
frame as a PNG. The tools Resident owns media publication through RTDF.
Codex exposes `publish_media` for generated files, and the adapter also
captures app-server `imageGeneration` items when they contain image bytes.
The web app saves ordinary room history and media under its local data directory,
which defaults to `%LOCALAPPDATA%\NormaHarness` on Windows. Set `NORMA_DATA_DIR`
to override it. Incognito rooms are excluded from these files. `NORMA_CHAT_RESTORE`
can import an older JSON room snapshot when no saved chat exists.

## Try it

Sign in once with `codex login`, then run a read-only round trip:

```console
cargo run -p norma-residents --example roundtrip -- . "Summarize this repository in one sentence."
```

The example uses `codex.cmd` on Windows and `codex` elsewhere. Set
`NORMA_CODEX_BIN` to a specific Codex executable when multiple CLI versions
are installed. Set `NORMA_CODEX_THREAD_ID` to a previous result's `thread_id`
to resume that conversation. GPT-6 Luna requires a client and account that
support it.

Set `NORMA_CODEX_WORKSPACE_WRITE=1` for the example to permit Codex to edit
the selected workspace. The default example run is read-only.

`CodexResidentConfig` defaults to `gpt-6-luna`, the local Codex reasoning effort, `ReadOnly`, a 16-event mailbox,
and a five-minute turn timeout. Override `model` for another available Codex
model, or set `NORMA_CODEX_EFFORT` for a different effort in the example or web app.
Set `sandbox = CodexSandbox::WorkspaceWrite` in an
application that explicitly allows Codex to edit its workspace. The adapter
uses `approvalPolicy = never` and declines unexpected approval requests; an
application needing interactive approvals must implement that client flow.

The app-server process belongs to `CodexResidentRuntime`. Call
`runtime.shutdown().await` to finish queued work, stop the process, and
unregister the precise Resident instance. The worker serializes turns on one
app-server connection. A failed or timed-out connection is replaced before the
next request. Only one `CodexResident` instance can be active in a process;
another `launch` call fails until that instance has fully stopped.

See the [official Codex App Server documentation](https://learn.chatgpt.com/docs/app-server)
and [Codex authentication documentation](https://learn.chatgpt.com/docs/auth).
