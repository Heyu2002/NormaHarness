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
    llm::{self, LlmChatTurnKind, LlmOrigin, LlmRoomKind, MemoryTrigger},
    media::MediaAsset,
    memory::{ExtractedMemory, MemoryManager},
    tools::{
        self, ChatContextResult, ChatContextToolRequest, MediaPublishRequest, MediaPublishResult,
        RoomMembersResult, RoomMembersToolRequest, ToolCatalogRequest, ToolCatalogResult,
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
    pending_context: StdMutex<HashMap<String, oneshot::Sender<ChatContextResult>>>,
    pending_catalog: StdMutex<HashMap<String, oneshot::Sender<ToolCatalogResult>>>,
    pending_media: StdMutex<HashMap<String, oneshot::Sender<MediaPublishResult>>>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct ConversationState {
    thread_id: Option<String>,
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
            if message.message().kind().as_str() == tools::CHAT_CONTEXT_TOOL_RESULT_KIND {
                if message.source().as_str() != tools::TOOL_RESIDENT_KEY {
                    return Err(MailboxError::new(
                        "chat-context result came from another Resident",
                    ));
                }
                let result: ChatContextResult =
                    serde_json::from_value(message.message().payload().clone()).map_err(
                        |error| MailboxError::new(format!("invalid chat-context result: {error}")),
                    )?;
                if let Some(reply) = self
                    .pending_context
                    .lock()
                    .expect("context pending mutex")
                    .remove(&result.call_id)
                {
                    let _ = reply.send(result);
                }
                return Ok(());
            }
            if message.message().kind().as_str() == tools::TOOL_CATALOG_RESULT_KIND {
                if message.source().as_str() != tools::TOOL_RESIDENT_KEY {
                    return Err(MailboxError::new("tool catalog came from another Resident"));
                }
                let result: ToolCatalogResult = serde_json::from_value(
                    message.message().payload().clone(),
                )
                .map_err(|error| MailboxError::new(format!("invalid tool catalog: {error}")))?;
                if let Some(reply) = self
                    .pending_catalog
                    .lock()
                    .expect("catalog pending mutex")
                    .remove(&result.request_id)
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

/// Shared Codex execution state. This is not a Resident and is never registered
/// with RDF; each public Resident owns one independent instance of it.
struct CodexResidentCore {
    descriptor: ResidentDescriptor,
    mailbox: Arc<QueueMailbox>,
    registration: RegistrationSender,
    messages: MessageSender,
    config: CodexResidentConfig,
    instance_id: OnceLock<ResidentInstanceId>,
    inbound_gate: Arc<dyn Gate>,
}

impl std::fmt::Debug for CodexResidentCore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexResidentCore")
            .field("descriptor", &self.descriptor)
            .field("instance_id", &self.instance_id.get())
            .finish_non_exhaustive()
    }
}

impl CodexResidentCore {
    async fn prepare(
        mut config: CodexResidentConfig,
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<(Arc<Self>, mpsc::Receiver<ResidentEvent>, CodexAppServer), CodexResidentError>
    {
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
        let server = CodexAppServer::start(&config.codex_program, &config.cwd)
            .await
            .map_err(|error| CodexResidentError::AppServer(error.to_string()))?;
        let (queue, receiver) = mpsc::channel(config.mailbox_capacity);
        let resident = Arc::new(Self {
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
                pending_context: StdMutex::new(HashMap::new()),
                pending_catalog: StdMutex::new(HashMap::new()),
                pending_media: StdMutex::new(HashMap::new()),
            }),
            registration,
            messages,
            config,
            instance_id: OnceLock::new(),
            inbound_gate: Arc::new(llm::LlmContextGate),
        });
        Ok((resident, receiver, server))
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
        if request.origin.is_some() && request.source_resident.as_deref() != Some("chat.rooms") {
            return CodexTurnResult::failed(
                request.request_id,
                "chat origin must come from chat.rooms",
            );
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
        if let (Some(origin), Some(kind)) = (&request.origin, request.chat_turn_kind) {
            turn_request.prompt = chat_turn_notice(origin, kind, &request.prompt);
        }
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
            turn_request.thread_id = None;
            turn_request.prompt = format!(
                "之前的模型会话无法恢复。请先调用 read_chat_context 读取当前聊天室的历史，再继续回答当前请求。\n\n{}",
                turn_request.prompt,
            );
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
            chat_turn_kind: None,
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

    async fn load_tool_catalog(&self, request_id: &str) -> Result<Vec<Value>, String> {
        let (reply, receiver) = oneshot::channel();
        self.mailbox
            .pending_catalog
            .lock()
            .expect("catalog pending mutex")
            .insert(request_id.to_owned(), reply);
        let request = ToolCatalogRequest {
            request_id: request_id.to_owned(),
        };
        let flow = FlowMessage::new(
            MessageKind::new(tools::TOOL_CATALOG_REQUEST_KIND).expect("static kind"),
            serde_json::to_value(request).expect("serializable catalog request"),
        );
        let target = ResidentKey::new(tools::TOOL_RESIDENT_KEY).expect("static key");
        let instance_id = *self.instance_id.get().expect("registered");
        if let Err(error) = self.messages.send(instance_id, &target, flow).await {
            self.mailbox
                .pending_catalog
                .lock()
                .expect("catalog pending mutex")
                .remove(request_id);
            return Err(error.to_string());
        }
        let result = receiver.await;
        self.mailbox
            .pending_catalog
            .lock()
            .expect("catalog pending mutex")
            .remove(request_id);
        match result {
            Ok(result) if result.tools.is_empty() => Err("tool catalog is empty".into()),
            Ok(result) => Ok(result.tools),
            Err(_) => Err("tool catalog reply channel closed".into()),
        }
    }

    fn read_resident_memory_tool(&self, origin: Option<&LlmOrigin>) -> Result<Value, String> {
        let origin = origin.ok_or("memory tool requires a chat origin")?;
        if origin.incognito {
            return Err("long-term memory is unavailable in incognito rooms".into());
        }
        let memory = self
            .config
            .memory
            .as_ref()
            .ok_or("long-term memory is disabled")?;
        let (_, facts) = memory.hot().map_err(|error| error.to_string())?;
        Ok(json!({"facts": facts}))
    }

    async fn invoke_chat_context_tool(
        &self,
        turn: &CodexTurnRequest,
        call_id: &str,
        scope_all: bool,
        before_message_id: Option<u64>,
        limit: usize,
    ) -> Result<ChatContextResult, String> {
        let origin = turn
            .origin
            .as_ref()
            .ok_or("chat-context tool requires a chat origin")?;
        if call_id.trim().is_empty() || !(1..=200).contains(&limit) {
            return Err("invalid chat-context tool arguments".into());
        }
        let mut visible_through = HashMap::new();
        for event in &turn.context {
            visible_through
                .entry(event.room_id)
                .and_modify(|last: &mut u64| *last = (*last).max(event.id))
                .or_insert(event.id);
        }
        let request = ChatContextToolRequest {
            call_id: call_id.to_owned(),
            room_id: origin.room_id,
            visible_through,
            scope_all,
            before_message_id,
            limit,
        };
        let (reply, receiver) = oneshot::channel();
        self.mailbox
            .pending_context
            .lock()
            .expect("context pending mutex")
            .insert(call_id.to_owned(), reply);
        let flow = FlowMessage::new(
            MessageKind::new(tools::CHAT_CONTEXT_TOOL_REQUEST_KIND).expect("static kind"),
            serde_json::to_value(request).expect("serializable context request"),
        );
        let target = ResidentKey::new(tools::TOOL_RESIDENT_KEY).expect("static key");
        let instance_id = *self.instance_id.get().expect("registered");
        if let Err(error) = self.messages.send(instance_id, &target, flow).await {
            self.mailbox
                .pending_context
                .lock()
                .expect("context pending mutex")
                .remove(call_id);
            return Err(error.to_string());
        }
        let result = receiver.await;
        self.mailbox
            .pending_context
            .lock()
            .expect("context pending mutex")
            .remove(call_id);
        match result {
            Ok(result) if result.room_id == origin.room_id => Ok(result),
            Ok(_) => Err("chat-context result belongs to another room".into()),
            Err(_) => Err("chat-context reply channel closed".into()),
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

/// Original Codex Resident. Only one instance of this type may be active.
pub struct CodexResident {
    _instance_lease: CodexInstanceLease,
    core: Arc<CodexResidentCore>,
}

impl std::fmt::Debug for CodexResident {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexResident")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl Resident for CodexResident {
    fn descriptor(&self) -> ResidentDescriptor {
        self.core.descriptor.clone()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.core.mailbox.clone()
    }

    fn inbound_gate(&self) -> Option<Arc<dyn Gate>> {
        Some(self.core.inbound_gate.clone())
    }
}

impl CodexResident {
    /// Prepare the Codex process before advertising this Resident in RDF.
    pub async fn launch(
        config: CodexResidentConfig,
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<CodexResidentRuntime, CodexResidentError> {
        let instance_lease = CodexInstanceLease::acquire()?;
        let (core, receiver, server) =
            CodexResidentCore::prepare(config, registration, messages).await?;
        let resident = Arc::new(Self {
            _instance_lease: instance_lease,
            core: core.clone(),
        });
        let receipt = core.registration.register(resident.clone()).await?;
        core.instance_id
            .set(receipt.instance_id())
            .expect("Codex Resident registers only once");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let worker = tokio::spawn(core.run(receiver, server, shutdown_rx));
        Ok(CodexResidentRuntime {
            resident,
            shutdown: Some(shutdown),
            worker,
        })
    }
}

/// Separate Resident for GPT-5.6 Luna, with its own app-server and mailbox.
pub struct Codex56LunaResident {
    core: Arc<CodexResidentCore>,
}

impl std::fmt::Debug for Codex56LunaResident {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Codex56LunaResident")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl Resident for Codex56LunaResident {
    fn descriptor(&self) -> ResidentDescriptor {
        self.core.descriptor.clone()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.core.mailbox.clone()
    }

    fn inbound_gate(&self) -> Option<Arc<dyn Gate>> {
        Some(self.core.inbound_gate.clone())
    }
}

impl Codex56LunaResident {
    pub async fn launch(
        mut config: CodexResidentConfig,
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<Codex56LunaResidentRuntime, CodexResidentError> {
        config.model = Some("gpt-5.6-luna".into());
        let (core, receiver, server) =
            CodexResidentCore::prepare(config, registration, messages).await?;
        let resident = Arc::new(Self { core: core.clone() });
        let receipt = core.registration.register(resident.clone()).await?;
        core.instance_id
            .set(receipt.instance_id())
            .expect("GPT-5.6 Luna Resident registers only once");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let worker = tokio::spawn(core.run(receiver, server, shutdown_rx));
        Ok(Codex56LunaResidentRuntime {
            resident,
            shutdown: Some(shutdown),
            worker,
        })
    }
}

fn chat_turn_notice(origin: &LlmOrigin, kind: LlmChatTurnKind, trigger: &str) -> String {
    let place = if origin.kind == LlmRoomKind::Group {
        "群聊"
    } else {
        "私聊"
    };
    let action = match kind {
        LlmChatTurnKind::Direct => "有一条需要你回复的消息。",
        LlmChatTurnKind::Contribution => "有一条需要你独立回复的消息；不要假设其他成员已回答。",
        LlmChatTurnKind::Mention => trigger,
    };
    let routing = if origin.kind == LlmRoomKind::Group {
        "若答案已完整，请以 @你 开头直接回答用户；需要其他成员继续时，可以 @对应成员。除非确实需要继续处理，否则不要随意 @。"
    } else {
        ""
    };
    format!(
        "{place} {}（房间 ID {}）{action}请先调用 read_chat_context 读取本房间的消息，再作答。{routing}",
        serde_json::to_string(&origin.room_name).expect("room name is serializable"),
        origin.room_id,
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
        *self
            .resident
            .core
            .instance_id
            .get()
            .expect("registered runtime")
    }

    #[must_use]
    pub fn resident(&self) -> &Arc<CodexResident> {
        &self.resident
    }

    pub async fn shutdown(mut self) -> Result<(), CodexResidentError> {
        let instance_id = self.instance_id();
        self.resident.core.mailbox.close();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let worker_result = self.worker.await;
        let unregister_result = self
            .resident
            .core
            .registration
            .unregister(instance_id)
            .await;
        worker_result?;
        unregister_result?;
        Ok(())
    }
}

/// Owner of the GPT-5.6 Luna Resident's execution.
pub struct Codex56LunaResidentRuntime {
    resident: Arc<Codex56LunaResident>,
    shutdown: Option<oneshot::Sender<()>>,
    worker: JoinHandle<()>,
}

impl std::fmt::Debug for Codex56LunaResidentRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Codex56LunaResidentRuntime")
            .field("resident", &self.resident)
            .finish_non_exhaustive()
    }
}

impl Codex56LunaResidentRuntime {
    #[must_use]
    pub fn instance_id(&self) -> ResidentInstanceId {
        *self
            .resident
            .core
            .instance_id
            .get()
            .expect("registered runtime")
    }

    #[must_use]
    pub fn resident(&self) -> &Arc<Codex56LunaResident> {
        &self.resident
    }

    pub async fn shutdown(mut self) -> Result<(), CodexResidentError> {
        let instance_id = self.instance_id();
        self.resident.core.mailbox.close();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let worker_result = self.worker.await;
        let unregister_result = self
            .resident
            .core
            .registration
            .unregister(instance_id)
            .await;
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
    use super::{CodexInstanceLease, chat_turn_notice};
    use crate::llm::{LlmChatTurnKind, LlmOrigin, LlmRoomKind};

    #[test]
    fn a_second_codex_instance_is_rejected_until_the_first_is_released() {
        let first = CodexInstanceLease::acquire().expect("first instance");
        assert!(CodexInstanceLease::acquire().is_err());
        drop(first);
        assert!(CodexInstanceLease::acquire().is_ok());
    }

    #[test]
    fn chat_notice_only_identifies_room_and_stage() {
        let origin = LlmOrigin {
            kind: LlmRoomKind::Group,
            room_id: 7,
            room_name: "设计讨论".into(),
            incognito: false,
        };
        let notice = chat_turn_notice(&origin, LlmChatTurnKind::Contribution, "");
        assert!(notice.contains("设计讨论"));
        assert!(notice.contains("房间 ID 7"));
        assert!(notice.contains("独立回复"));
        assert!(notice.contains("read_chat_context"));
        assert!(!notice.contains("current_request"));
        assert!(notice.contains("@你"));
        let mention = chat_turn_notice(&origin, LlmChatTurnKind::Mention, "alpha @ 了你。");
        assert!(mention.contains("alpha @ 了你"));
    }
}
