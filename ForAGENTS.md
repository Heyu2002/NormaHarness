# ForAGENTS: Norma Harness development map

[English project overview](./README.md) · [Chinese project overview](./README.zh-CN.md)

Use this document to locate code, preserve protocol boundaries, and understand why those boundaries exist. It describes the current implementation; source code is authoritative for exact signatures. The two READMEs explain the project to people. [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) contains a deeper account of ownership and routing.

## Find the owning layer first

| Change | Primary code | Verification |
| --- | --- | --- |
| Registration, capability discovery, instance identity | `src/rdf.rs`, `src/resident_store.rs`, `src/resident.rs`, `src/id.rs` | `tests/data_flows.rs` |
| Message routing, Gates, mailboxes | `src/rtdf.rs`, `src/gate.rs`, `src/mailbox.rs`, `src/message.rs` | `tests/data_flows.rs` |
| LLM request/result protocol and origin validation | `residents/src/llm.rs` | `residents/tests/chat_rooms.rs` |
| Rooms, group scheduling, mentions, archive, sleep | `residents/src/chat/mod.rs` | `residents/tests/chat_rooms.rs` |
| Persistent chat snapshots | `residents/src/chat/storage.rs` | `residents/tests/chat_rooms.rs` |
| Codex processes, provider threads, dynamic tool calls | `residents/src/codex/mod.rs`, `app_server.rs`, `protocol.rs` | Codex unit tests, `chat_rooms.rs` |
| Tool catalog and request forwarding | `residents/src/tools/mod.rs` | `residents/tests/chat_rooms.rs` |
| Image/GIF validation and storage; long-term memory | `residents/src/media.rs`, `memory.rs` | Module tests, `chat_rooms.rs` |
| HTTP API, startup composition, environment settings | `web/src/main.rs` | `cargo test -p norma-web`, HTTP checks |
| Page markup and interaction | `web/static/index.html`, `style.css`, `app.js` | `node --check web/static/app.js`, browser checks |

The workspace has three crates. Root `norma-harness` is the reusable framework; `residents/` contains concrete participants; `web/` composes them into a chat application. A chat feature that crosses crates still belongs in a concrete Resident, not in the root framework.

## Hard boundaries of the framework

### 1. A Resident owns its execution and state

The `Resident` trait exposes only a stable `ResidentDescriptor`, a `MailboxAddress`, and optional outbound/inbound `Gate` implementations. It has no shared `start/stop` method, retry engine, rollback engine, or session manager. A new Resident may own its own queue, child process, tasks, and shutdown sequence.

**Why:** the framework connects different kinds of services and cannot assume one business lifecycle. Putting an adapter's execution policy into RDF or RTDF would impose that policy on every Resident.

The application injects only `RegistrationSender` and `MessageSender` when constructing a Resident. These cloneable handles own channel senders only. A Resident should not retain strong references to `NormaHarness`, RDF, RTDF, or `ResidentStore`. Otherwise the ownership path `RDF → Store → Resident` could loop back into the framework.

### 2. RDF is the sole registration authority

After its mailbox and Gates are ready, a Resident calls `RegistrationSender::register(Arc<Self>)`. RDF reads the public endpoints from the actual instance, serializes same-name checks, assigns a process-local, non-reusable `ResidentInstanceId`, and updates both the registration directory and private `ResidentStore`. A successful receipt contains the instance ID, a snapshot of previously registered Residents, and any registration-notice failures. Notice failures are reported; they do not undo a successful registration.

`ResidentStore` strongly owns successful registrations by instance ID. It does not check names, route messages, or hold business state. Name and capability queries use RDF's registration directory.

**Why:** name uniqueness and instance ownership must be committed in the same registration command. A new instance may reuse a name after the old one unregisters, but the old instance ID must never regain sending rights. The ID marks a generation within this process; it is not a cross-process security credential.

### 3. RTDF performs exactly one delivery hop

The send entry point is `send(source_instance_id, target_key, FlowMessage)`. Its fixed path is:

```text
RDF validates the source and resolves the target
→ source outbound Gate
→ target inbound Gate
→ target Mailbox::deliver
```

`Mailbox::deliver` is synchronous and should only enqueue an event. Do not run a model or wait for long-running business work inside it. A successful `send().await` confirms mailbox acceptance only. An LLM result arrives in a separate `llm.turn.result` message correlated by `request_id`.

**Why:** RTDF does not understand chat, tools, or model semantics and does not perform business retries. It dispatches deliveries independently, so a Gate can send another message without blocking a global delivery loop. A Gate or mailbox failure ends that delivery without changing registration state.

### 4. Unregistration is the Resident's final framework step

Stop accepting new work, drain or cancel owned work, guard public endpoints, then unregister the exact instance ID. RDF does not wait for the Resident's execution function to return: that function may itself be waiting for the `unregister` response. A delivery that already copied Gate or mailbox handles can finish after unregistration, so the concrete Resident must make late calls safe.

## The actual chat and model path

`web/src/main.rs` creates `NormaHarness` inside Tokio, registers `chat.rooms`, `tools.rooms`, and two separate Codex Residents, then starts Axum. Currently `codex` uses GPT-6 Luna; `codex-5.6-luna` uses a separate `Codex56LunaResident` with GPT-5.6 Luna. Each has its own app-server process and provider-thread mapping. The original `CodexResident` still has a single-active-instance limit. Do not remove that guard to turn an old instance into a second Resident.

One chat turn follows this route:

```text
Browser → HTTP → ChatResident room
→ RTDF: llm.turn.request → target LLM Resident
→ RTDF: llm.turn.result → ChatResident
→ room messages / SSE → browser
```

The shared protocol lives in `residents/src/llm.rs`. A Resident advertising the `llm` capability must handle `llm.turn.request` and return `llm.turn.result` with the same `request_id`. Requests carry the room `origin` and turn `context`. `LlmContextGate` validates the origin and stamps `source_resident` from RTDF's actual sender. Do not trust a claimed source in an arbitrary payload.

`ChatResident` owns rooms, messages, busy state, and reply correlation. With no specific mention, it starts independent initial turns for every group member; with a mention, it initially calls only the named members. Initial turns for different members run concurrently. **Turns for the same Resident remain serialized across rooms** to preserve that Resident's context order. Model-to-model mention follow-ups run after the initial turns, with a limit of 16 follow-up turns per user message. A complete answer prefixed with the literal `@你` is displayed to the user as a summary.

These scheduling rules belong in `residents/src/chat/mod.rs`, not in `web/static/app.js` or RTDF. The browser presents state and sends requests; it does not choose the next model to speak.

## Model tools and context boundaries

Codex receives a short room notice instructing it to call `read_chat_context`. It does not receive the entire chat JSON envelope as user-message text. `tools.rooms` supplies a tool catalog over RTDF; the Codex adapter registers `read_chat_context`, `list_group_members`, `read_resident_memory`, and `publish_media` as app-server `dynamicTools`. This is currently an experimental Codex app-server interface. Check `residents/src/codex/app_server.rs` and actual app-server responses when changing it.

When adding a model tool, review at least four places: the catalog and message kinds in `tools/mod.rs`, dispatch in `codex/app_server.rs`, the Resident that owns the real data, and a cross-Resident test. A forwarding tool service must not grant access merely because a model supplied a room ID.

`read_chat_context` is bounded by the current turn's snapshot. `ChatResident` checks that the caller belongs to the room, that returned message IDs do not exceed the visible upper bound, and that incognito rooms remain isolated. An ordinary room may request messages from the model's other ordinary rooms; an incognito room can read only itself. Keep authorization at the real data owner so tool arguments cannot expand model access.

`publish_media` sends actual image bytes to the media service. A filename in model text does not create an attachment. The limits are four files per message and 8 MiB per file; PNG, JPEG, WebP, and GIF are supported. Ordinary media is persistent, incognito media uses a temporary directory, and the browser keeps GIF animation while the model sees its first frame.

Long-term memory is off by default. `ChatResident` triggers extraction only after room sleep or model-context compaction. `MemoryManager` stores facts and their cache/hot status; the model reads them on demand through `read_resident_memory`. Incognito rooms do not enter memory. The current web app injects one shared `MemoryManager` into both Codex Residents; do not describe them as having separate memory stores.

## How to make common changes

1. **Add a regular Resident:** start with the [round-trip example](./residents/examples/roundtrip.rs). Implement the minimal `Resident` interface in `residents/`, prepare the mailbox and optional Gates, then register the real `Arc`. Keep the receipt's instance ID. Let the worker process its queued events; drain it and unregister during shutdown. Test identity and routing in `tests/data_flows.rs` or an appropriate integration test.
2. **Add an LLM provider:** implement the request/result protocol in `llm.rs`, advertise `llm`, validate `origin`, correlate asynchronous replies by `request_id`, and own provider-session continuity. Cover solo chat, no-mention concurrency, directed mentions, follow-ups, and incognito scope in `chat_rooms.rs`.
3. **Change group behavior:** trace a full turn through `ChatResident::send_user_message_with_media`, `run_turn`, and `ask`. First determine whether the issue concerns parallel members, serial turns for one member, or mention follow-ups. Do not solve it with a frontend delay or a global RTDF lock.
4. **Change a model tool:** keep the model-facing schema, Codex dispatch, RTDF tool messages, and data-owner validation in sync. Return chat information through real tools such as `read_chat_context`; do not fabricate tool results in a prompt.
5. **Change the website:** `web/src/main.rs` owns HTTP and runtime composition. Static assets are compiled into the executable with `include_str!`; after changing `web/static/*`, rebuild and restart the service before checking the page. Filtering, presentation, and English/Chinese UI strings live in `app.js`; markup keys live in `index.html`. The page language defaults to English and is stored per browser in `localStorage`. `ChatResident` remains the authority for rooms.
6. **Change persistence or incognito behavior:** inspect `chat/storage.rs`, `media.rs`, `memory.rs`, Codex conversation mappings, and restore paths together. Ordinary rooms must remain resumable; incognito rooms must not enter Norma's ordinary snapshot or long-term memory.

## Run and verify

The workspace uses Rust 2024 edition and requires Rust 1.85 or newer. Standard checks:

```console
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
node --check web/static/app.js
```

For documentation-only changes, verify links, commands, and paths against current code, then run `git diff --check`. For chat protocol, tool, or incognito changes, prefer the cross-Resident tests in `residents/tests/chat_rooms.rs`. For RDF/RTDF changes, use `tests/data_flows.rs`. `live_codex_can_call_group_member_tool` requires an authenticated local Codex CLI and a real model turn; it is ignored by default, so its skipped status is not evidence of a live call.

The local website binds to `0.0.0.0:3000` by default and currently has no website user authentication. `web/src/main.rs` reads these startup settings: `NORMA_WEB_BIND`, `NORMA_CODEX_CWD`, `NORMA_CODEX_BIN`, `NORMA_CODEX_EFFORT`, `NORMA_CODEX_WORKSPACE_WRITE`, `NORMA_DATA_DIR`, and `NORMA_THREAD_IDLE_SECS`. Codex uses a read-only sandbox by default; only `NORMA_CODEX_WORKSPACE_WRITE=1` enables workspace edits.
