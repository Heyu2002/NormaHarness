//! Runs the locally authenticated Codex CLI as a Norma Resident.
//!
//! The Resident owns a Codex app-server child process. RDF and RTDF only see
//! its stable descriptor, mailbox, and channel-based messages.

mod app_server;
mod protocol;

use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use norma_harness::{
    CapabilityKey, FlowMessage, Gate, Mailbox, MailboxAddress, MailboxError, MessageKind,
    MessageSender, RegistrationError, RegistrationSender, Resident, ResidentDescriptor,
    ResidentEvent, ResidentInstanceId, ResidentKey, RoutedMessage,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    llm::{self, LlmOrigin, LlmRoomKind, MemoryTrigger},
    media::MediaAsset,
    memory::{ExtractedMemory, MemoryManager},
    tools::{
        self, MediaPublishRequest, MediaPublishResult, RoomMembersResult, RoomMembersToolRequest,
    },
};
use app_server::{AppServerError, CodexAppServer};
pub use protocol::{
    CodexTurnRequest, CodexTurnResult, CodexTurnStatus, TURN_REQUEST_KIND, TURN_RESULT_KIND,
};

static CODEX_INSTANCE_LIVE: AtomicBool = AtomicBool::new(false);

struct CodexInstanceLease;

impl CodexInstanceLease {
    fn acquire() -> Result<Self, CodexResidentError> {
        CODEX_INSTANCE_LIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                CodexResidentError::Config("only one Codex Resident may be active".into())
            })?;
        Ok(Self)
    }
}

impl Drop for CodexInstanceLease {
    fn drop(&mut self) {
        CODEX_INSTANCE_LIVE.store(false, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexSandbox {
    ReadOnly,
    WorkspaceWrite,
}

impl CodexSandbox {
    fn policy(self, cwd: &str) -> Value {
        match self {
            Self::ReadOnly => json!({"type": "readOnly"}),
            Self::WorkspaceWrite => json!({
                "type": "workspaceWrite",
                "writableRoots": [cwd],
                "networkAccess": false
            }),
        }
    }
}

/// The child process inherits the caller's Codex login and environment.
#[derive(Debug, Clone)]
pub struct CodexResidentConfig {
    pub key: ResidentKey,
    pub cwd: PathBuf,
    pub codex_program: PathBuf,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub sandbox: CodexSandbox,
    pub mailbox_capacity: usize,
    pub memory: Option<Arc<MemoryManager>>,
    pub conversation_path: Option<PathBuf>,
}

impl CodexResidentConfig {
    #[must_use]
    pub fn new(key: ResidentKey, cwd: impl Into<PathBuf>) -> Self {
        Self {
            key,
            cwd: cwd.into(),
            codex_program: PathBuf::from(if cfg!(windows) { "codex.cmd" } else { "codex" }),
            model: Some("gpt-6-luna".into()),
            effort: None,
            sandbox: CodexSandbox::ReadOnly,
            mailbox_capacity: 16,
            memory: None,
            conversation_path: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum CodexResidentError {
    #[error("invalid Codex Resident configuration: {0}")]
    Config(String),
    #[error("Codex app-server failed: {0}")]
    AppServer(String),
    #[error("Codex Resident registration failed: {0}")]
    Registration(#[from] RegistrationError),
    #[error("Codex Resident worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
}

struct QueueMailbox {
    queue: mpsc::Sender<ResidentEvent>,
    accepting: AtomicBool,
    pending_tools: StdMutex<HashMap<String, oneshot::Sender<RoomMembersResult>>>,
    pending_media: StdMutex<HashMap<String, oneshot::Sender<MediaPublishResult>>>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct ConversationState {
    thread_id: Option<String>,
    last_seen_context_id: u64,
    last_memory_revision: u64,
}

fn load_conversations(path: &PathBuf) -> io::Result<HashMap<u64, ConversationState>> {
    let backup = path.with_extension("json.bak");
    for candidate in [path, &backup] {
        match fs::read(candidate) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(state) => return Ok(state),
                Err(error) if candidate == path && backup.exists() => {
                    eprintln!("Codex conversation map is invalid ({error}); trying backup");
                }
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(HashMap::new())
}

fn save_conversations(
    path: &PathBuf,
    conversations: &HashMap<u64, ConversationState>,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let pending = path.with_extension("json.tmp");
    let backup = path.with_extension("json.bak");
    let encoded = serde_json::to_vec(conversations)?;
    use io::Write;
    let mut file = fs::File::create(&pending)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    drop(file);
    if path.exists() {
        if backup.exists() {
            fs::remove_file(&backup)?;
        }
        fs::rename(path, &backup)?;
    }
    if let Err(error) = fs::rename(&pending, path) {
        if backup.exists() {
            let _ = fs::rename(&backup, path);
        }
        return Err(error);
    }
    Ok(())
}

impl QueueMailbox {
    fn close(&self) {
        self.accepting.store(false, Ordering::Release);
    }
}

impl Mailbox for QueueMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(MailboxError::new("Codex Resident is stopping"));
        }
        if matches!(event, ResidentEvent::ResidentRegistered(_)) {
            return Ok(());
        }
        if let ResidentEvent::Message(message) = &event {
            if message.message().kind().as_str() == tools::MEDIA_PUBLISH_RESULT_KIND {
                if message.source().as_str() != tools::TOOL_RESIDENT_KEY {
                    return Err(MailboxError::new("media result came from another Resident"));
                }
                let result: MediaPublishResult = serde_json::from_value(
                    message.message().payload().clone(),
                )
                .map_err(|error| MailboxError::new(format!("invalid media result: {error}")))?;
                if let Some(reply) = self
                    .pending_media
                    .lock()
                    .expect("media pending mutex")
                    .remove(&result.call_id)
                {
                    let _ = reply.send(result);
                }
                return Ok(());
            }
            if message.message().kind().as_str() == tools::ROOM_MEMBERS_TOOL_RESULT_KIND {
                if message.source().as_str() != tools::TOOL_RESIDENT_KEY {
                    return Err(MailboxError::new(
                        "room-members result came from another Resident",
                    ));
                }
                let result: RoomMembersResult =
                    serde_json::from_value(message.message().payload().clone()).map_err(
                        |error| MailboxError::new(format!("invalid room-members result: {error}")),
                    )?;
                if let Some(reply) = self
                    .pending_tools
                    .lock()
                    .expect("tool pending mutex")
                    .remove(&result.call_id)
                {
                    let _ = reply.send(result);
                }
                return Ok(());
            }
        }
        self.queue
            .try_send(event)
            .map_err(|error| MailboxError::new(format!("Codex Resident mailbox: {error}")))
    }
}

/// The concrete service stored by RDF. The worker and Codex child are owned
/// by [`CodexResidentRuntime`], so RDF never controls their lifecycle.
pub struct CodexResident {
    _instance_lease: CodexInstanceLease,
    descriptor: ResidentDescriptor,
    mailbox: Arc<QueueMailbox>,
    registration: RegistrationSender,
    messages: MessageSender,
    config: CodexResidentConfig,
    instance_id: OnceLock<ResidentInstanceId>,
    inbound_gate: Arc<dyn Gate>,
}

impl std::fmt::Debug for CodexResident {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexResident")
            .field("descriptor", &self.descriptor)
            .field("instance_id", &self.instance_id.get())
            .finish_non_exhaustive()
    }
}

impl Resident for CodexResident {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }

    fn inbound_gate(&self) -> Option<Arc<dyn Gate>> {
        Some(self.inbound_gate.clone())
    }
}

impl CodexResident {
    /// Prepare the Codex process before advertising this Resident in RDF.
    pub async fn launch(
        mut config: CodexResidentConfig,
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<CodexResidentRuntime, CodexResidentError> {
        if config.mailbox_capacity == 0 {
            return Err(CodexResidentError::Config(
                "mailbox_capacity must be positive".into(),
            ));
        }
        if !config.cwd.is_absolute() {
            config.cwd = std::env::current_dir()
                .map_err(|error| CodexResidentError::Config(error.to_string()))?
                .join(&config.cwd);
        }
        if !config.cwd.is_dir() {
            return Err(CodexResidentError::Config(format!(
                "working directory does not exist: {}",
                config.cwd.display()
            )));
        }
        let instance_lease = CodexInstanceLease::acquire()?;
        let server = CodexAppServer::start(&config.codex_program, &config.cwd)
            .await
            .map_err(|error| CodexResidentError::AppServer(error.to_string()))?;
        let (queue, receiver) = mpsc::channel(config.mailbox_capacity);
        let resident = Arc::new(Self {
            _instance_lease: instance_lease,
            descriptor: ResidentDescriptor::new(
                config.key.clone(),
                [
                    CapabilityKey::new("codex.agent").expect("static capability is valid"),
                    CapabilityKey::new(llm::LLM_CAPABILITY).expect("static capability is valid"),
                ],
            ),
            mailbox: Arc::new(QueueMailbox {
                queue,
                accepting: AtomicBool::new(true),
                pending_tools: StdMutex::new(HashMap::new()),
                pending_media: StdMutex::new(HashMap::new()),
            }),
            registration,
            messages,
            config,
            instance_id: OnceLock::new(),
            inbound_gate: Arc::new(llm::LlmContextGate),
        });
        let receipt = resident.registration.register(resident.clone()).await?;
        resident
            .instance_id
            .set(receipt.instance_id())
            .expect("Codex Resident registers only once");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let worker_resident = resident.clone();
        let worker = tokio::spawn(async move {
            worker_resident.run(receiver, server, shutdown_rx).await;
        });
        Ok(CodexResidentRuntime {
            resident,
            shutdown: Some(shutdown),
            worker,
        })
    }

    async fn run(
        self: Arc<Self>,
        mut receiver: mpsc::Receiver<ResidentEvent>,
        server: CodexAppServer,
        mut shutdown: oneshot::Receiver<()>,
    ) {
        let mut server = Some(server);
        let mut conversations = self
            .config
            .conversation_path
            .as_ref()
            .and_then(|path| match load_conversations(path) {
                Ok(state) => Some(state),
                Err(error) => {
                    eprintln!("could not restore Codex conversation map: {error}");
                    None
                }
            })
            .unwrap_or_default();
        let mut private_rooms = HashSet::new();
        let mut stopping = false;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown, if !stopping => {
                    stopping = true;
                    receiver.close();
                }
                event = receiver.recv() => {
                    let Some(ResidentEvent::Message(message)) = event else {
                        if event.is_none() { break; }
                        continue;
                    };
                    let request_kind = message.message().kind().as_str();
                    if request_kind == llm::MEMORY_TRIGGER_KIND {
                        self.process_memory_trigger(&mut server, message).await;
                        continue;
                    }
                    if request_kind != TURN_REQUEST_KIND && request_kind != llm::TURN_REQUEST_KIND {
                        continue;
                    }
                    let result_kind = if request_kind == llm::TURN_REQUEST_KIND {
                        llm::TURN_RESULT_KIND
                    } else {
                        TURN_RESULT_KIND
                    };
                    let target = message.source().clone();
                    let room_id = message.message().payload()["origin"]["room_id"].as_u64().unwrap_or(0);
                    let incognito = message.message().payload()["origin"]["incognito"].as_bool().unwrap_or(false);
                    if incognito { private_rooms.insert(room_id); }
                    let conversation = conversations.entry(room_id).or_default();
                    let result = self.handle(message, &mut server, conversation).await;
                    if !incognito && result.status == CodexTurnStatus::Completed {
                        if let Some(path) = &self.config.conversation_path {
                            let persisted = conversations.iter().filter(|(id, _)| **id != 0 && !private_rooms.contains(id))
                                .map(|(id, state)| (*id, state.clone())).collect();
                            if let Err(error) = save_conversations(path, &persisted) {
                                eprintln!("could not save Codex conversation map: {error}");
                            }
                        }
                    }
                    let payload = serde_json::to_value(result)
                        .expect("Codex result is serializable");
                    let reply = FlowMessage::new(
                        MessageKind::new(result_kind).expect("static message kind is valid"),
                        payload,
                    );
                    let instance_id = *self.instance_id.get().expect("registered before run");
                    if let Err(error) = self.messages.send(instance_id, &target, reply).await {
                        eprintln!("Codex Resident could not reply to {target}: {error}");
                    }
                }
            }
        }
    }

    async fn handle(
        &self,
        message: RoutedMessage,
        server: &mut Option<CodexAppServer>,
        conversation: &mut ConversationState,
    ) -> CodexTurnResult {
        let value = message.message().payload();
        let request_id = value["request_id"].as_str().unwrap_or_default().to_owned();
        let request: CodexTurnRequest = match serde_json::from_value(value.clone()) {
            Ok(request) => request,
            Err(error) => return CodexTurnResult::failed(request_id, error.to_string()),
        };
        if let Err(error) = request.validate() {
            return CodexTurnResult::failed(request.request_id, error);
        }
        if request.thread_id.as_ref().is_some_and(|thread_id| {
            conversation
                .thread_id
                .as_ref()
                .is_some_and(|current| current != thread_id)
        }) {
            return CodexTurnResult::failed(
                request.request_id,
                "this Codex Resident already has a different conversation thread",
            );
        }
        let mut turn_request = request.clone();
        turn_request.thread_id = conversation.thread_id.clone().or(request.thread_id.clone());
        turn_request.prompt = prompt_with_new_context(
            &request,
            conversation.last_seen_context_id,
            self.config.key.as_str(),
        );
        let mut memory_revision = None;
        if let Some(memory) = self.config.memory.as_ref().filter(|_| {
            !request
                .origin
                .as_ref()
                .is_some_and(|origin| origin.incognito)
        }) {
            match memory.hot() {
                Ok((revision, hot)) if revision != conversation.last_memory_revision => {
                    if !hot.is_empty() {
                        turn_request.prompt = format!(
                            "当前 Resident 的长期记忆如下。它们可能已过时，若与当前用户消息冲突，以当前消息为准。\n{}\n\n{}",
                            hot.iter()
                                .enumerate()
                                .map(|(index, fact)| format!("{}. {fact}", index + 1))
                                .collect::<Vec<_>>()
                                .join("\n"),
                            turn_request.prompt,
                        );
                    }
                    memory_revision = Some(revision);
                }
                Ok(_) => {}
                Err(error) => eprintln!("could not read Resident memory: {error}"),
            }
        }
        turn_request
            .context
            .retain(|event| event.id > conversation.last_seen_context_id);
        if server.is_none() {
            match CodexAppServer::start(&self.config.codex_program, &self.config.cwd).await {
                Ok(restarted) => *server = Some(restarted),
                Err(error) => {
                    return CodexTurnResult::failed(request.request_id, error.to_string());
                }
            }
        }
        let mut result = server
            .as_mut()
            .expect("server just started")
            .run_turn(
                &turn_request,
                self,
                &self.config.cwd,
                self.config.model.as_deref(),
                self.config.effort.as_deref(),
                self.config.sandbox,
            )
            .await;
        if matches!(&result, Err(AppServerError::Protocol(error)) if error.starts_with("thread/resume:"))
        {
            eprintln!("provider thread cannot be resumed; rebuilding from local room history");
            conversation.thread_id = None;
            conversation.last_seen_context_id = 0;
            turn_request.thread_id = None;
            turn_request.context = request.context.clone();
            turn_request.prompt = format!(
                "之前的模型会话无法恢复。以下是本地保存的完整历史，请在当前房间继续对话。历史消息：{}\n当前请求：{}",
                serde_json::to_string(&request.context).unwrap_or_default(),
                request.prompt,
            );
            if memory_revision.is_some() {
                if let Some(memory) = &self.config.memory {
                    if let Ok((_, hot)) = memory.hot() {
                        if !hot.is_empty() {
                            turn_request.prompt =
                                format!("长期记忆：{}\n{}", hot.join("\n"), turn_request.prompt);
                        }
                    }
                }
            }
            result = server
                .as_mut()
                .expect("server still active")
                .run_turn(
                    &turn_request,
                    self,
                    &self.config.cwd,
                    self.config.model.as_deref(),
                    self.config.effort.as_deref(),
                    self.config.sandbox,
                )
                .await;
        }
        match result {
            Ok(result) => {
                if let Some(thread_id) = &result.thread_id {
                    conversation.thread_id = Some(thread_id.clone());
                }
                if result.status == CodexTurnStatus::Completed {
                    if let Some(revision) = memory_revision {
                        conversation.last_memory_revision = revision;
                    }
                    conversation.last_seen_context_id = request
                        .context
                        .iter()
                        .map(|event| event.id)
                        .max()
                        .unwrap_or(conversation.last_seen_context_id)
                        .max(conversation.last_seen_context_id);
                }
                if result.compacted {
                    conversation.last_memory_revision = 0;
                }
                result
            }
            Err(error) => {
                *server = None;
                CodexTurnResult::failed(request.request_id, error.to_string())
            }
        }
    }

    async fn process_memory_trigger(
        &self,
        server: &mut Option<CodexAppServer>,
        message: RoutedMessage,
    ) {
        let Some(memory) = &self.config.memory else {
            return;
        };
        let trigger: MemoryTrigger =
            match serde_json::from_value(message.message().payload().clone()) {
                Ok(trigger) => trigger,
                Err(error) => {
                    eprintln!("invalid memory trigger: {error}");
                    return;
                }
            };
        let events = memory.unprocessed(trigger.room_id, &trigger.context);
        if events.is_empty() {
            return;
        }
        let known = memory.known_keys();
        let input = events
            .iter()
            .map(|event| {
                json!({
                    "id": event.id, "role": event.role, "author": event.author, "text": event.text,
                })
            })
            .collect::<Vec<_>>();
        let prompt = format!(
            "你正在为 Resident 提取长期记忆。只提取用户明确表达、对未来对话有用的信息；不要编造，也不要把临时任务步骤当成长期事实。相同事实沿用已有 key。每个事实的 evidence_ids 列出本次所有明确提到它的用户消息 ID，不要使用 Agent 消息作为依据。只输出 JSON：{{\"facts\":[{{\"key\":\"稳定的简短键\",\"text\":\"记忆内容\",\"evidence_ids\":[消息ID]}}]}}。没有有效内容则输出 {{\"facts\":[]}}。已有键：{}。本次消息：{}",
            serde_json::to_string(&known).unwrap_or_default(),
            serde_json::to_string(&input).unwrap_or_default(),
        );
        let request = CodexTurnRequest {
            request_id: format!(
                "memory-{}-{}",
                trigger.room_id,
                events.last().map_or(0, |event| event.id)
            ),
            prompt,
            origin: None,
            source_resident: Some("chat.rooms".into()),
            thread_id: None,
            context: events.clone(),
        };
        if server.is_none() {
            match CodexAppServer::start(&self.config.codex_program, &self.config.cwd).await {
                Ok(restarted) => *server = Some(restarted),
                Err(error) => {
                    eprintln!("memory extractor could not start Codex: {error}");
                    return;
                }
            }
        }
        let result = server
            .as_mut()
            .expect("server just started")
            .run_turn(
                &request,
                self,
                &self.config.cwd,
                self.config.model.as_deref(),
                self.config.effort.as_deref(),
                self.config.sandbox,
            )
            .await;
        let response = match result {
            Ok(result) if result.status == CodexTurnStatus::Completed => result.final_response,
            Ok(result) => {
                eprintln!("memory extraction failed: {:?}", result.error);
                return;
            }
            Err(error) => {
                *server = None;
                eprintln!("memory extraction failed: {error}");
                return;
            }
        };
        let Some(response) = response else { return };
        let parsed = serde_json::from_str::<ExtractedMemory>(&response).or_else(|_| {
            let start = response
                .find('{')
                .ok_or_else(|| serde_json::Error::io(std::io::Error::other("no JSON object")))?;
            let end = response
                .rfind('}')
                .ok_or_else(|| serde_json::Error::io(std::io::Error::other("no JSON object")))?;
            serde_json::from_str(&response[start..=end])
        });
        match parsed {
            Ok(facts) => {
                if let Err(error) = memory.apply(trigger.room_id, &events, facts) {
                    eprintln!("could not save extracted memory: {error}");
                }
            }
            Err(error) => eprintln!("could not parse extracted memory: {error}"),
        }
    }

    async fn invoke_room_members_tool(
        &self,
        origin: Option<&LlmOrigin>,
        call_id: &str,
    ) -> Result<RoomMembersResult, String> {
        let origin = origin.ok_or("room-members tool requires a chat origin")?;
        if origin.kind != LlmRoomKind::Group {
            return Err("room-members tool is available only in a group chat".into());
        }
        if call_id.trim().is_empty() {
            return Err("missing tool call ID".into());
        }
        let (reply, receiver) = oneshot::channel();
        self.mailbox
            .pending_tools
            .lock()
            .expect("tool pending mutex")
            .insert(call_id.to_owned(), reply);
        let request = RoomMembersToolRequest {
            call_id: call_id.to_owned(),
            room_id: origin.room_id,
        };
        let flow = FlowMessage::new(
            MessageKind::new(tools::ROOM_MEMBERS_TOOL_REQUEST_KIND).expect("static kind"),
            serde_json::to_value(request).expect("serializable tool request"),
        );
        let target = ResidentKey::new(tools::TOOL_RESIDENT_KEY).expect("static key");
        let instance_id = *self.instance_id.get().expect("registered");
        if let Err(error) = self.messages.send(instance_id, &target, flow).await {
            self.mailbox
                .pending_tools
                .lock()
                .expect("tool pending mutex")
                .remove(call_id);
            return Err(error.to_string());
        }
        let result = receiver.await;
        self.mailbox
            .pending_tools
            .lock()
            .expect("tool pending mutex")
            .remove(call_id);
        match result {
            Ok(result) if result.room_id == origin.room_id => Ok(result),
            Ok(_) => Err("room-members result belongs to another group".into()),
            Err(_) => Err("room-members reply channel closed".into()),
        }
    }

    async fn invoke_publish_media_tool(
        &self,
        request: MediaPublishRequest,
    ) -> Result<MediaAsset, String> {
        if request.call_id.trim().is_empty() {
            return Err("missing tool call ID".into());
        }
        let (reply, receiver) = oneshot::channel();
        self.mailbox
            .pending_media
            .lock()
            .expect("media pending mutex")
            .insert(request.call_id.clone(), reply);
        let call_id = request.call_id.clone();
        let flow = FlowMessage::new(
            MessageKind::new(tools::MEDIA_PUBLISH_REQUEST_KIND).expect("static kind"),
            serde_json::to_value(request).expect("serializable media request"),
        );
        let target = ResidentKey::new(tools::TOOL_RESIDENT_KEY).expect("static key");
        let instance_id = *self.instance_id.get().expect("registered");
        if let Err(error) = self.messages.send(instance_id, &target, flow).await {
            self.mailbox
                .pending_media
                .lock()
                .expect("media pending mutex")
                .remove(&call_id);
            return Err(error.to_string());
        }
        let result = receiver.await;
        self.mailbox
            .pending_media
            .lock()
            .expect("media pending mutex")
            .remove(&call_id);
        match result {
            Ok(result) => result.asset.ok_or_else(|| {
                result
                    .error
                    .unwrap_or_else(|| "media publication failed".into())
            }),
            Err(_) => Err("media publication channel closed".into()),
        }
    }
}

fn prompt_with_new_context(
    request: &CodexTurnRequest,
    last_seen_context_id: u64,
    resident_key: &str,
) -> String {
    let mut unseen = request
        .context
        .iter()
        .filter(|event| event.id > last_seen_context_id)
        .filter(|event| {
            !(last_seen_context_id != 0
                && (event.role == "agent" || event.role == "summary")
                && event.author == resident_key)
        })
        .collect::<Vec<_>>();
    unseen.sort_by_key(|event| event.id);
    if request.origin.is_none() && unseen.is_empty() {
        return request.prompt.clone();
    }
    let envelope = json!({
        "source": {
            "resident": request.source_resident,
            "origin": request.origin,
        },
        "new_events": unseen,
        "current_request": request.prompt,
    });
    format!(
        "你是同一个持续参与对话的 Resident。以下 JSON 是框架提供的上下文：source.origin 标明当前私聊或群聊及群名，new_events 是你上次处理后在你参与的房间发生的消息。按顺序理解事件，只在当前房间回复。附件图片已作为本轮图像输入提供；GIF 输入保留原动画供聊天室查看，模型收到首帧。需要知道当前群成员时调用 list_group_members 工具。若生成图片或 GIF，必须调用 publish_media 工具发布文件内容，只有工具成功返回后用户才能看见附件。\n{}",
        envelope
    )
}

/// Owner of the Resident's execution. Call `shutdown` to complete its
/// lifecycle; dropping the harness later is not a substitute for unregister.
pub struct CodexResidentRuntime {
    resident: Arc<CodexResident>,
    shutdown: Option<oneshot::Sender<()>>,
    worker: JoinHandle<()>,
}

impl std::fmt::Debug for CodexResidentRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexResidentRuntime")
            .field("resident", &self.resident)
            .finish_non_exhaustive()
    }
}

impl CodexResidentRuntime {
    #[must_use]
    pub fn instance_id(&self) -> ResidentInstanceId {
        *self.resident.instance_id.get().expect("registered runtime")
    }

    #[must_use]
    pub fn resident(&self) -> &Arc<CodexResident> {
        &self.resident
    }

    pub async fn shutdown(mut self) -> Result<(), CodexResidentError> {
        let instance_id = self.instance_id();
        self.resident.mailbox.close();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let worker_result = self.worker.await;
        let unregister_result = self.resident.registration.unregister(instance_id).await;
        worker_result?;
        unregister_result?;
        Ok(())
    }
}

/// Construct the RTDF request envelope without exposing transport details to
/// callers of the concrete Resident.
#[must_use]
pub fn turn_request_message(request: &CodexTurnRequest) -> FlowMessage {
    FlowMessage::new(
        MessageKind::new(TURN_REQUEST_KIND).expect("static message kind is valid"),
        serde_json::to_value(request).expect("Codex request is serializable"),
    )
}

#[cfg(test)]
mod tests {
    use super::{CodexInstanceLease, CodexTurnRequest, prompt_with_new_context};
    use crate::llm::LlmContextMessage;

    #[test]
    fn a_second_codex_instance_is_rejected_until_the_first_is_released() {
        let first = CodexInstanceLease::acquire().expect("first instance");
        assert!(CodexInstanceLease::acquire().is_err());
        drop(first);
        assert!(CodexInstanceLease::acquire().is_ok());
    }

    #[test]
    fn one_resident_sees_new_messages_from_every_room_it_joins() {
        let request = CodexTurnRequest {
            request_id: "r1".into(),
            prompt: "回答当前群聊".into(),
            origin: Some(crate::llm::LlmOrigin {
                kind: crate::llm::LlmRoomKind::Group,
                room_id: 2,
                room_name: "群聊".into(),
                incognito: false,
            }),
            source_resident: Some("chat.rooms".into()),
            thread_id: None,
            context: vec![
                LlmContextMessage {
                    id: 3,
                    room_id: 2,
                    room_name: "群聊".into(),
                    role: "agent".into(),
                    author: "beta".into(),
                    text: "群里的新信息".into(),
                    created_at_ms: 0,
                    attachments: Vec::new(),
                },
                LlmContextMessage {
                    id: 1,
                    room_id: 1,
                    room_name: "私聊".into(),
                    role: "user".into(),
                    author: "你".into(),
                    text: "私聊里的旧信息".into(),
                    created_at_ms: 0,
                    attachments: Vec::new(),
                },
                LlmContextMessage {
                    id: 2,
                    room_id: 1,
                    room_name: "私聊".into(),
                    role: "agent".into(),
                    author: "codex".into(),
                    text: "我已经答过".into(),
                    created_at_ms: 0,
                    attachments: Vec::new(),
                },
            ],
        };
        let prompt = prompt_with_new_context(&request, 0, "codex");
        assert!(prompt.find("私聊里的旧信息").unwrap() < prompt.find("群里的新信息").unwrap());
        assert!(prompt.contains("我已经答过"));
        assert!(prompt.contains("回答当前群聊"));
        let prompt = prompt_with_new_context(&request, 1, "codex");
        assert!(!prompt.contains("私聊里的旧信息"));
        assert!(prompt.contains("群里的新信息"));
    }
}
