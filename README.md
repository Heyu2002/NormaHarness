# Norma Harness

**English** · [简体中文](./README.zh-CN.md) · [ForAGENTS: development guide](./ForAGENTS.md)

Norma Harness is a Rust framework for connecting independent Agents within one process, with a local chat app you can run today. Each participant is a **Resident**: it may be an LLM, a chat room coordinator, or a tool service. Residents own their work and state; the framework discovers them and delivers messages between them.

If you want different models to think in the same room, mention one another, and keep the door open for your own Agents, this project provides a working starting point.

## What it can do

| Capability | What you can do |
| --- | --- |
| Solo and group chat | Click an online model to start a solo chat, or select at least two models for a group. The current web app starts separate GPT-6 Luna and GPT-5.6 Luna Residents. |
| Parallel answers and follow-ups | Without a specific @ mention, all group members begin answering concurrently. A mention initially addresses the named member. Models can mention other models to continue the discussion and start a complete answer to the user with `@你`. |
| Context through model tools | The current Codex Residents call `read_chat_context` after a room notification to retrieve real messages instead of receiving a full JSON history inside a user message. They can also look up group members and read long-term memory when needed. |
| Images and GIFs | Send PNG, JPEG, WebP, and GIF files. A model can publish generated images as previewable, downloadable chat attachments. |
| History and archives | Ordinary chats and attachments are stored locally and can resume after closing the page or restarting the service. Search, filter, and restore archived chats in Settings. |
| Incognito and optional memory | Choose incognito for new rooms so Norma does not save their chat snapshot or long-term memory. Long-term memory is off by default; when enabled, it extracts facts after room sleep or model context compaction. |

## Why this design matters

**Participants can evolve independently.** The chat app discovers LLM Residents by capability. A new model implementation can join by following the shared turn protocol; it does not need a special branch inside the framework. The framework does not take ownership of provider threads, retries, or business state.

**Collaboration is more than a fixed answer chain.** Group members first give independent answers in parallel, then use mentions for further exchanges. A member can address the user when it considers the answer complete. This keeps distinct viewpoints while allowing the discussion to move forward.

**Models fetch context through tools.** The model receives a short room notice and callable tools. Chat history, group membership, and media have explicit owners. History reads check room membership, what was visible at the start of the turn, and incognito boundaries. That makes new tools easier to add and the model's available context easier to inspect.

Underneath are two narrow data flows: **RDF** registers Residents and discovers capabilities; **RTDF** delivers one message hop between Residents. Business rules stay with each Resident, so the same foundation can connect services beyond chat.

## Run it locally

You need Rust **1.85+**, an installed and signed-in Codex CLI, and a local account with access to the selected models. Run `codex login`, then from the repository root:

```console
cargo run -p norma-web
```

Open [http://127.0.0.1:3000](http://127.0.0.1:3000). The web server binds to all IPv4 interfaces by default and has no website user authentication, so expose it only on a trusted network. To bind only to this machine:

```console
NORMA_WEB_BIND=127.0.0.1:3000 cargo run -p norma-web
```

That environment-variable syntax is for a Unix-like shell. In PowerShell, set `$env:NORMA_WEB_BIND = "127.0.0.1:3000"` before running `cargo run -p norma-web`.

In the app, click a model for solo chat. Create a group with both models to compare an unaddressed message, a message that @ mentions one model, and model-to-model follow-ups. The “Incognito mode” checkbox below “Create group” applies to new rooms; existing rooms retain their original mode.

Ordinary rooms, messages, media, and provider conversation mappings are stored locally. On Windows the default directory is `%LOCALAPPDATA%\NormaHarness`; elsewhere it uses the XDG/HOME data directory. Set `NORMA_DATA_DIR` to change it. Incognito only limits Norma's own persistence; the model provider and operating system may retain processing records.

Common settings:

| Environment variable | Purpose |
| --- | --- |
| `NORMA_WEB_BIND` | Listen address; defaults to `0.0.0.0:3000`. |
| `NORMA_CODEX_CWD` | Working directory used by Codex Residents; defaults to the current directory. |
| `NORMA_CODEX_EFFORT` | Override the local Codex reasoning effort. |
| `NORMA_CODEX_WORKSPACE_WRITE=1` | Allow Codex to edit the chosen workspace; read-only by default. |
| `NORMA_DATA_DIR` | Local directory for ordinary chat, media, memory, and provider conversation mappings. |

## Use it as a framework

`norma-harness` is the registration and message delivery library. `norma-residents` contains the Codex, chat, and tool implementations. To try a round trip without starting the website, run the [example](./residents/examples/roundtrip.rs):

```console
cargo run -p norma-residents --example roundtrip -- . "Summarize this repository in one sentence."
```

To add a Resident, change a model tool, or understand the framework constraints, read [ForAGENTS.md](./ForAGENTS.md). The [architecture notes](./docs/ARCHITECTURE.md) explain ownership and registration/delivery invariants in detail; [residents/README.md](./residents/README.md) covers the concrete Codex Resident protocols.

Licensed under Apache-2.0.
