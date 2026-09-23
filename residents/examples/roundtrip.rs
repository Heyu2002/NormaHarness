use std::{error::Error, sync::Arc};

use norma_harness::{
    Mailbox, MailboxAddress, MailboxError, MessageSender, NormaHarness, RegistrationSender,
    Resident, ResidentDescriptor, ResidentEvent, ResidentInstanceId, ResidentKey,
};
use norma_residents::codex::{
    CodexResident, CodexResidentConfig, CodexSandbox, CodexTurnRequest, CodexTurnResult,
    CodexTurnStatus, TURN_RESULT_KIND, turn_request_message,
};
use tokio::sync::mpsc;

struct CallerMailbox(mpsc::Sender<ResidentEvent>);

impl Mailbox for CallerMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        if matches!(event, ResidentEvent::ResidentRegistered(_)) {
            return Ok(());
        }
        self.0
            .try_send(event)
            .map_err(|error| MailboxError::new(error.to_string()))
    }
}

struct Caller {
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
    registration: RegistrationSender,
    messages: MessageSender,
}

impl Resident for Caller {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }
}

impl Caller {
    async fn send(
        &self,
        instance_id: ResidentInstanceId,
        request: &CodexTurnRequest,
    ) -> Result<(), Box<dyn Error>> {
        self.messages
            .send(
                instance_id,
                &ResidentKey::new("codex")?,
                turn_request_message(request),
            )
            .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let cwd = args
        .next()
        .ok_or("usage: roundtrip <directory> <prompt...>")?;
    let prompt = args.collect::<Vec<_>>().join(" ");
    if prompt.trim().is_empty() {
        return Err("prompt must not be empty".into());
    }

    let harness = NormaHarness::new();
    let (events, mut inbox) = mpsc::channel(8);
    let caller = Arc::new(Caller {
        descriptor: ResidentDescriptor::new(ResidentKey::new("caller")?, []),
        mailbox: Arc::new(CallerMailbox(events)),
        registration: harness.registration_sender(),
        messages: harness.message_sender(),
    });
    let caller_id = caller
        .registration
        .register(caller.clone())
        .await?
        .instance_id();

    let mut config = CodexResidentConfig::new(ResidentKey::new("codex")?, cwd);
    if let Some(program) = std::env::var_os("NORMA_CODEX_BIN") {
        config.codex_program = program.into();
    }
    if let Ok(effort) = std::env::var("NORMA_CODEX_EFFORT") {
        config.effort = Some(effort);
    }
    if std::env::var("NORMA_CODEX_WORKSPACE_WRITE").as_deref() == Ok("1") {
        config.sandbox = CodexSandbox::WorkspaceWrite;
    }
    let codex = CodexResident::launch(
        config,
        harness.registration_sender(),
        harness.message_sender(),
    )
    .await?;
    caller
        .send(
            caller_id,
            &CodexTurnRequest {
                request_id: "roundtrip-1".into(),
                prompt,
                origin: None,
                source_resident: None,
                thread_id: std::env::var("NORMA_CODEX_THREAD_ID").ok(),
                context: Vec::new(),
            },
        )
        .await?;

    let result: CodexTurnResult = loop {
        let Some(event) = inbox.recv().await else {
            return Err("caller inbox closed before Codex replied".into());
        };
        if let ResidentEvent::Message(message) = event {
            if message.message().kind().as_str() == TURN_RESULT_KIND {
                break serde_json::from_value(message.message().payload().clone())?;
            }
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);

    codex.shutdown().await?;
    caller.registration.unregister(caller_id).await?;
    if result.status != CodexTurnStatus::Completed {
        return Err(result
            .error
            .unwrap_or_else(|| "Codex turn failed".into())
            .into());
    }
    Ok(())
}
