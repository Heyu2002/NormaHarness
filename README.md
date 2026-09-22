# Norma Harness

**English** | [简体中文](./README.zh-CN.md)

Norma Harness is a small Rust library for connecting independently implemented, in-process services called **Residents**.

It defines two data flows:

- **RDF (Registration Data Flow)** registers Residents, enforces unique names, publishes capabilities and delivery endpoints, and announces new registrations.
- **RTDF (Runtime Data Flow)** moves one message from a registered source to a registered target through a fixed delivery path.

A Resident keeps control of its own execution model, state, concurrency, dependencies, retry policy, cancellation, rollback, and shutdown boundary. Norma Harness only provides registration and one-hop delivery.

## Architecture

```mermaid
flowchart LR
    App["Application"] --> Harness["NormaHarness"]

    Harness --> RDF["RDF<br/>registration authority"]
    Harness --> RTDF["RTDF<br/>message delivery"]

    RDF --> Directory["Registration directory<br/>name · capability · endpoints"]
    RDF --> Store["ResidentStore<br/>instance ownership"]
    Store --> Resident["Resident instances"]

    RTDF -->|resolve route| RDF

    Resident -. "RegistrationSender<br/>(channel only)" .-> RDF
    Resident -. "MessageSender<br/>(channel only)" .-> RTDF
```

`NormaHarness` is the composition root. It starts RDF and RTDF and gives the application two cloneable channel handles to inject into Residents. The handles do not own RDF, RTDF, the Store, or the application object.

| Component | Responsibility |
| --- | --- |
| `Resident` | Implements one service and exposes its name, capabilities, mailbox, and optional Gates. |
| `NormaHarness` | Constructs and owns the framework data flows and exposes their narrow entry points. |
| `RDF` | Serializes registration changes, enforces name uniqueness, maintains the public directory, and announces new Residents. |
| `ResidentStore` | Strongly owns successfully registered Resident instances by instance ID. It is private framework storage. |
| `RTDF` | Resolves one target and performs one-hop delivery. It does not execute Resident business logic. |
| `Gate` | Optionally validates or transforms a message at a Resident-owned transport boundary. |
| `Mailbox` | Accepts a `ResidentEvent` by enqueueing it for later Resident processing. |

The model has four core invariants:

1. A Resident name identifies at most one live registration.
2. Every successful registration receives a process-local, non-reusable `ResidentInstanceId`.
3. A message always passes through the source outbound Gate, the target inbound Gate, and then the target mailbox.
4. Business state and business lifecycle remain inside the concrete Resident.

## Add the dependency

For a Git dependency:

```toml
[dependencies]
norma-harness = { git = "https://github.com/Heyu2002/NormaHarness" }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`NormaHarness::new()` starts background tasks and must be called inside a Tokio runtime.

## Minimal example

The example below defines a Resident that owns the two injected framework ports, registers itself when ready, sends one message, and unregisters only after its own work has stopped.

```rust
use std::{error::Error, sync::Arc};

use norma_harness::{
    CapabilityKey, FlowMessage, IdentifierError, Mailbox, MailboxAddress,
    MailboxError, MessageKind, MessageSender, NormaHarness, RegistrationError,
    RegistrationReceipt, RegistrationSender, Resident, ResidentDescriptor,
    ResidentEvent, ResidentInstanceId, ResidentKey, RouteError,
};
use serde_json::json;

struct PrintMailbox;

impl Mailbox for PrintMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        // A real mailbox should enqueue and return immediately.
        println!("{event:?}");
        Ok(())
    }
}

struct Service {
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
    registration: RegistrationSender,
    messages: MessageSender,
}

impl Service {
    fn new(
        name: &str,
        capability: &str,
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<Self, IdentifierError> {
        Ok(Self {
            descriptor: ResidentDescriptor::new(
                ResidentKey::new(name)?,
                [CapabilityKey::new(capability)?],
            ),
            mailbox: Arc::new(PrintMailbox),
            registration,
            messages,
        })
    }

    async fn register(
        self: &Arc<Self>,
    ) -> Result<RegistrationReceipt, RegistrationError> {
        self.registration.register(Arc::clone(self)).await
    }

    async fn send(
        &self,
        instance_id: ResidentInstanceId,
        target: &ResidentKey,
    ) -> Result<(), RouteError> {
        self.messages
            .send(
                instance_id,
                target,
                FlowMessage::new(
                    MessageKind::new("request").expect("static message kind"),
                    json!({ "prompt": "store this value" }),
                ),
            )
            .await
    }

    async fn unregister(
        &self,
        instance_id: ResidentInstanceId,
    ) -> Result<(), RegistrationError> {
        self.registration.unregister(instance_id).await.map(|_| ())
    }
}

impl Resident for Service {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let registration = harness.registration_sender();
    let messages = harness.message_sender();

    let llm = Arc::new(Service::new(
        "llm",
        "generate",
        registration.clone(),
        messages.clone(),
    )?);
    let storage = Arc::new(Service::new(
        "storage",
        "store",
        registration,
        messages,
    )?);

    let llm_receipt = llm.register().await?;
    let storage_receipt = storage.register().await?;
    let storage_key = storage.descriptor().key().clone();

    llm.send(llm_receipt.instance_id(), &storage_key).await?;

    // Each Resident reaches its own safe shutdown boundary first.
    llm.unregister(llm_receipt.instance_id()).await?;
    storage
        .unregister(storage_receipt.instance_id())
        .await?;

    Ok(())
}
```

## Registration and delivery sequence

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant Source as Source Resident
    participant Reg as RegistrationSender
    participant RDF
    participant Store as ResidentStore
    participant Msg as MessageSender
    participant RTDF
    participant Target as Target Resident

    App->>Source: construct(registration, messages)
    Source->>Reg: register(Arc<Self>)
    Reg->>RDF: Register command
    RDF->>Source: read descriptor, mailbox, Gates
    RDF->>RDF: validate instance and unique name
    RDF->>Store: retain instance by InstanceId
    RDF->>RDF: commit registration record
    RDF->>Target: ResidentRegistered(Source)
    RDF-->>Source: receipt + existing Residents

    Source->>Msg: send(source InstanceId, target name, message)
    Msg->>RTDF: Route command
    RTDF->>RDF: resolve source and target
    RDF-->>RTDF: source Gate, target Gate, target mailbox
    RTDF->>Source: outbound Gate
    Source-->>RTDF: accepted or transformed message
    RTDF->>Target: inbound Gate
    Target-->>RTDF: accepted or transformed message
    RTDF->>Target: enqueue Message event
    RTDF-->>Source: delivery result

    Source->>Source: stop new work and drain owned work
    Source->>Reg: unregister(InstanceId)
    Reg->>RDF: Unregister command
    RDF->>RDF: remove registration
    RDF->>Store: release strong instance reference
    RDF-->>Source: unregistration result
```

## Registration contract

A Resident registers when all of its public endpoints are ready. The registration request carries the real `Arc<dyn Resident>`, so RDF reads the descriptor, mailbox, and Gates from the instance instead of trusting a separately assembled record.

A successful `RegistrationReceipt` contains:

- the assigned `ResidentInstanceId`;
- a snapshot of Residents that were already registered;
- any failures encountered while notifying existing Residents about the newcomer.

RDF is the only same-name authority. Concurrent attempts to register the same name are serialized, and only one can succeed. A notification failure is reported in the receipt; it does not roll back the new registration or remove the Resident whose mailbox rejected the notice.

The descriptor and delivery endpoints must remain stable until unregistration. A service that needs a different public identity or different endpoints should retire the old registration and register a new instance.

## Routing contract

A send request contains three values:

```text
source ResidentInstanceId
target ResidentKey
FlowMessage { kind, JSON payload }
```

RTDF performs exactly one hop:

```text
RDF route resolution
→ source outbound Gate
→ target inbound Gate
→ target mailbox
```

The source is identified by its exact instance ID. Reusing the same Resident name later does not revive an old ID. The target is selected by its current unique name.

Both Gates may accept, reject, or transform the message. The mailbox receives a `ResidentEvent::Message(RoutedMessage)` only after both Gates succeed. Gate or mailbox failure ends that delivery and returns a `RouteError`; it does not change either Resident's registration.

`Mailbox::deliver` is synchronous by design and should only enqueue an event. Resident business work should run in the Resident's own task or executor.

## Lifetime and concurrency

Unregistration is the Resident's final framework operation:

```text
stop accepting new work
→ drain or cancel Resident-owned work
→ close or guard public endpoints
→ unregister the exact InstanceId
→ return from Resident execution
```

RDF does not wait for a Resident execution function during unregistration. Waiting in both directions would create a lifecycle deadlock. Removing a Store reference also does not force object destruction: unrelated `Arc` references may keep the Rust value alive, but the old instance no longer has a valid RDF identity and cannot start new RTDF sends.

A delivery that already copied its Gate and mailbox handles may finish concurrently with unregistration. Each Resident must make late calls safe at its own shutdown boundary.

RDF processes registration commands serially. RTDF dispatches deliveries independently, so a Gate may await or initiate another send without blocking one global delivery loop. Dropping `NormaHarness` shuts down the framework cores even if cloned Sender handles still exist.

## Deliberate limits

Norma Harness does not provide:

- Session, Thread, Turn, or context compression;
- workflow orchestration or business result interpretation;
- a generic state machine;
- checkpoints, persistence, Ledger, or durable execution;
- framework retry, cancellation, compensation, fallback, or rollback policies;
- Resident factories, a Resident Host, WASM hosting, or an ABI;
- hot-update activation, versioned rollout, or historical version rollback;
- multi-hop routing, remote transport, or network discovery.

Those capabilities can be implemented by Residents or by separate upper layers without expanding RDF or RTDF.

## Repository layout

```text
crates/norma-harness/src/
├── application.rs     # composition root and injected ports
├── rdf.rs             # registration commands and directory
├── resident_store.rs  # strong ownership of registered instances
├── rtdf.rs            # one-hop delivery pipeline
├── resident.rs        # Resident interface and registration events
├── gate.rs            # outbound and inbound message boundaries
├── mailbox.rs         # Resident-owned delivery address
├── message.rs         # flow and routed messages
├── id.rs              # names, capability keys, and instance IDs
└── error.rs           # boundary errors
```

The detailed invariants are documented in [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).

## Development

```console
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

## License

Apache-2.0.
