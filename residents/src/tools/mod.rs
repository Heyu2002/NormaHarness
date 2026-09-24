//! Model tools provided by a Resident and executed through RTDF.

use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::media::{MediaAsset, MediaStore};
use norma_harness::{
    CapabilityKey, FlowMessage, Mailbox, MailboxAddress, MailboxError, MessageKind, MessageSender,
    RegistrationError, RegistrationSender, Resident, ResidentDescriptor, ResidentEvent,
    ResidentInstanceId, ResidentKey,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{sync::mpsc, task::JoinHandle};

pub const TOOL_RESIDENT_KEY: &str = "tools.rooms";
pub const TOOL_CAPABILITY: &str = "tool.room_members";
pub const ROOM_MEMBERS_TOOL_REQUEST_KIND: &str = "tool.room_members.request";
pub const ROOM_MEMBERS_TOOL_RESULT_KIND: &str = "tool.room_members.result";
pub(crate) const ROOM_MEMBERS_QUERY_KIND: &str = "chat.room_members.query";
pub(crate) const ROOM_MEMBERS_RESULT_KIND: &str = "chat.room_members.result";
pub const MEDIA_PUBLISH_REQUEST_KIND: &str = "tool.media.publish.request";
pub const MEDIA_PUBLISH_RESULT_KIND: &str = "tool.media.publish.result";
pub const TOOL_CATALOG_REQUEST_KIND: &str = "tool.catalog.request";
pub const TOOL_CATALOG_RESULT_KIND: &str = "tool.catalog.result";
pub const CHAT_CONTEXT_TOOL_REQUEST_KIND: &str = "tool.chat_context.request";
pub const CHAT_CONTEXT_TOOL_RESULT_KIND: &str = "tool.chat_context.result";
pub(crate) const CHAT_CONTEXT_QUERY_KIND: &str = "chat.context.query";
pub(crate) const CHAT_CONTEXT_RESULT_KIND: &str = "chat.context.result";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCatalogRequest {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCatalogResult {
    pub request_id: String,
    pub tools: Vec<Value>,
}

fn model_tool_catalog() -> Vec<Value> {
    vec![
        json!({
            "type": "function",
            "name": "read_chat_context",
            "description": "读取聊天室真实消息。收到房间通知后先调用此工具读取当前消息和相关历史。默认只读当前房间最近 50 条；scope=all 可读你参与的其他普通房间，无痕只读本房间。可用 before_message_id 向前翻页。",
            "inputSchema": {"type": "object", "properties": {
                "scope": {"type": "string", "enum": ["current", "all"], "description": "默认 current；all 包含你参与的其他普通房间"},
                "before_message_id": {"type": "integer", "minimum": 1, "description": "可选：只返回此消息 ID 之前的消息，用于向前翻页"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "description": "最多返回的消息数，默认 50"}
            }, "additionalProperties": false}
        }),
        json!({
            "type": "function",
            "name": "list_group_members",
            "description": "查询当前群聊中的 LLM Resident 成员。仅在当前请求来自群聊时使用。无需参数。",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
        }),
        json!({
            "type": "function",
            "name": "read_resident_memory",
            "description": "按需读取当前 Resident 保存的长期记忆，例如已记录的用户偏好或背景。无痕房间不可用。无需参数。",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
        }),
        json!({
            "type": "function",
            "name": "publish_media",
            "description": "把生成的 PNG、JPEG、WebP 或 GIF 图片发布为当前聊天的可预览、可下载附件。图片生成后必须调用此工具；只在回复中写文件名不会把文件交给用户。单次回复最多 4 个，每个最多 8 MiB。",
            "inputSchema": {"type": "object", "properties": {
                "filename": {"type": "string"},
                "mime_type": {"type": "string", "enum": ["image/png", "image/jpeg", "image/webp", "image/gif"]},
                "base64": {"type": "string", "description": "完整图片文件内容的标准 base64，不包含 data URL 前缀"}
            }, "required": ["filename", "mime_type", "base64"], "additionalProperties": false}
        }),
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatContextToolRequest {
    pub call_id: String,
    pub room_id: u64,
    pub visible_through: HashMap<u64, u64>,
    #[serde(default)]
    pub scope_all: bool,
    pub before_message_id: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChatContextQuery {
    pub request: ChatContextToolRequest,
    pub requester: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatContextResult {
    pub call_id: String,
    pub room_id: u64,
    pub room_name: Option<String>,
    pub messages: Vec<crate::llm::LlmContextMessage>,
    pub has_earlier: bool,
    pub next_before_message_id: Option<u64>,
    pub error: Option<String>,
}

impl ChatContextResult {
    fn failed(request: &ChatContextToolRequest, error: impl Into<String>) -> Self {
        Self {
            call_id: request.call_id.clone(),
            room_id: request.room_id,
            room_name: None,
            messages: Vec::new(),
            has_earlier: false,
            next_before_message_id: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaPublishRequest {
    pub call_id: String,
    pub filename: String,
    pub mime_type: String,
    pub base64: String,
    #[serde(default)]
    pub incognito: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaPublishResult {
    pub call_id: String,
    pub asset: Option<MediaAsset>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomMembersToolRequest {
    pub call_id: String,
    pub room_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RoomMembersQuery {
    pub call_id: String,
    pub room_id: u64,
    pub requester: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomMembersResult {
    pub call_id: String,
    pub room_id: u64,
    pub room_name: Option<String>,
    pub members: Vec<String>,
    pub error: Option<String>,
}

impl RoomMembersResult {
    fn failed(request: &RoomMembersToolRequest, error: impl Into<String>) -> Self {
        Self {
            call_id: request.call_id.clone(),
            room_id: request.room_id,
            room_name: None,
            members: Vec::new(),
            error: Some(error.into()),
        }
    }
}

struct ToolMailbox(mpsc::UnboundedSender<ResidentEvent>);

impl Mailbox for ToolMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        self.0
            .send(event)
            .map_err(|error| MailboxError::new(error.to_string()))
    }
}

pub struct RoomToolsResident {
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
    registration: RegistrationSender,
    messages: MessageSender,
    instance_id: OnceLock<ResidentInstanceId>,
    next_query: AtomicU64,
    media: Arc<MediaStore>,
    private_media: Arc<MediaStore>,
}

impl std::fmt::Debug for RoomToolsResident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoomToolsResident")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl Resident for RoomToolsResident {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }
    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }
}

impl RoomToolsResident {
    pub async fn launch(
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<RoomToolsResidentRuntime, RegistrationError> {
        let media = Arc::new(MediaStore::create().expect("create temporary media store"));
        Self::launch_with_media(registration, messages, media).await
    }

    pub async fn launch_with_media(
        registration: RegistrationSender,
        messages: MessageSender,
        media: Arc<MediaStore>,
    ) -> Result<RoomToolsResidentRuntime, RegistrationError> {
        Self::launch_with_media_stores(registration, messages, media.clone(), media).await
    }

    pub async fn launch_with_media_stores(
        registration: RegistrationSender,
        messages: MessageSender,
        media: Arc<MediaStore>,
        private_media: Arc<MediaStore>,
    ) -> Result<RoomToolsResidentRuntime, RegistrationError> {
        let (events, inbox) = mpsc::unbounded_channel();
        let resident = Arc::new(Self {
            descriptor: ResidentDescriptor::new(
                ResidentKey::new(TOOL_RESIDENT_KEY).expect("static key"),
                [CapabilityKey::new(TOOL_CAPABILITY).expect("static capability")],
            ),
            mailbox: Arc::new(ToolMailbox(events)),
            registration,
            messages,
            instance_id: OnceLock::new(),
            next_query: AtomicU64::new(1),
            media,
            private_media,
        });
        let receipt = resident.registration.register(resident.clone()).await?;
        resident
            .instance_id
            .set(receipt.instance_id())
            .expect("registers once");
        let worker_resident = resident.clone();
        let worker = tokio::spawn(async move { worker_resident.run(inbox).await });
        Ok(RoomToolsResidentRuntime { resident, worker })
    }

    async fn run(self: Arc<Self>, mut inbox: mpsc::UnboundedReceiver<ResidentEvent>) {
        let mut pending: HashMap<String, (ResidentKey, String)> = HashMap::new();
        let mut pending_context: HashMap<String, (ResidentKey, String)> = HashMap::new();
        while let Some(event) = inbox.recv().await {
            let ResidentEvent::Message(message) = event else {
                continue;
            };
            match message.message().kind().as_str() {
                TOOL_CATALOG_REQUEST_KIND => {
                    let Ok(request) = serde_json::from_value::<ToolCatalogRequest>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    let result = ToolCatalogResult {
                        request_id: request.request_id,
                        tools: model_tool_catalog(),
                    };
                    let flow = FlowMessage::new(
                        MessageKind::new(TOOL_CATALOG_RESULT_KIND).expect("static kind"),
                        serde_json::to_value(result).expect("serializable catalog"),
                    );
                    let instance_id = *self.instance_id.get().expect("registered");
                    let _ = self
                        .messages
                        .send(instance_id, message.source(), flow)
                        .await;
                }
                MEDIA_PUBLISH_REQUEST_KIND => {
                    let Ok(request) = serde_json::from_value::<MediaPublishRequest>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    let result = if request.call_id.trim().is_empty() {
                        Err("missing tool call ID".to_owned())
                    } else {
                        (if request.incognito {
                            &self.private_media
                        } else {
                            &self.media
                        })
                        .save_base64(
                            &request.filename,
                            &request.mime_type,
                            &request.base64,
                        )
                    };
                    let result = match result {
                        Ok(asset) => MediaPublishResult {
                            call_id: request.call_id,
                            asset: Some(asset),
                            error: None,
                        },
                        Err(error) => MediaPublishResult {
                            call_id: request.call_id,
                            asset: None,
                            error: Some(error),
                        },
                    };
                    let flow = FlowMessage::new(
                        MessageKind::new(MEDIA_PUBLISH_RESULT_KIND).expect("static kind"),
                        serde_json::to_value(result).expect("serializable media result"),
                    );
                    let instance_id = *self.instance_id.get().expect("registered");
                    let _ = self
                        .messages
                        .send(instance_id, message.source(), flow)
                        .await;
                }
                ROOM_MEMBERS_TOOL_REQUEST_KIND => {
                    let Ok(request) = serde_json::from_value::<RoomMembersToolRequest>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    if request.call_id.trim().is_empty() || request.room_id == 0 {
                        self.send_result(
                            message.source(),
                            RoomMembersResult::failed(
                                &request,
                                "invalid or duplicate room-members request",
                            ),
                        )
                        .await;
                        continue;
                    }
                    let relay_id = format!(
                        "members-{}",
                        self.next_query.fetch_add(1, Ordering::Relaxed)
                    );
                    pending.insert(
                        relay_id.clone(),
                        (message.source().clone(), request.call_id.clone()),
                    );
                    let query = RoomMembersQuery {
                        call_id: relay_id.clone(),
                        room_id: request.room_id,
                        requester: message.source().to_string(),
                    };
                    let flow = FlowMessage::new(
                        MessageKind::new(ROOM_MEMBERS_QUERY_KIND).expect("static kind"),
                        serde_json::to_value(query).expect("serializable query"),
                    );
                    let target = ResidentKey::new("chat.rooms").expect("static key");
                    let instance_id = *self.instance_id.get().expect("registered");
                    if let Err(error) = self.messages.send(instance_id, &target, flow).await {
                        pending.remove(&relay_id);
                        self.send_result(
                            message.source(),
                            RoomMembersResult::failed(&request, error.to_string()),
                        )
                        .await;
                    }
                }
                ROOM_MEMBERS_RESULT_KIND if message.source().as_str() == "chat.rooms" => {
                    let Ok(mut result) = serde_json::from_value::<RoomMembersResult>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    if let Some((target, call_id)) = pending.remove(&result.call_id) {
                        result.call_id = call_id;
                        self.send_result(&target, result).await;
                    }
                }
                CHAT_CONTEXT_TOOL_REQUEST_KIND => {
                    let Ok(request) = serde_json::from_value::<ChatContextToolRequest>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    if request.call_id.trim().is_empty()
                        || request.room_id == 0
                        || !request.visible_through.contains_key(&request.room_id)
                        || !(1..=200).contains(&request.limit)
                    {
                        self.send_context_result(
                            message.source(),
                            ChatContextResult::failed(&request, "invalid chat-context request"),
                        )
                        .await;
                        continue;
                    }
                    let relay_id = format!(
                        "context-{}",
                        self.next_query.fetch_add(1, Ordering::Relaxed)
                    );
                    pending_context.insert(
                        relay_id.clone(),
                        (message.source().clone(), request.call_id.clone()),
                    );
                    let mut forwarded = request.clone();
                    forwarded.call_id = relay_id.clone();
                    let query = ChatContextQuery {
                        request: forwarded,
                        requester: message.source().to_string(),
                    };
                    let flow = FlowMessage::new(
                        MessageKind::new(CHAT_CONTEXT_QUERY_KIND).expect("static kind"),
                        serde_json::to_value(query).expect("serializable context query"),
                    );
                    let target = ResidentKey::new("chat.rooms").expect("static key");
                    let instance_id = *self.instance_id.get().expect("registered");
                    if let Err(error) = self.messages.send(instance_id, &target, flow).await {
                        pending_context.remove(&relay_id);
                        self.send_context_result(
                            message.source(),
                            ChatContextResult::failed(&request, error.to_string()),
                        )
                        .await;
                    }
                }
                CHAT_CONTEXT_RESULT_KIND if message.source().as_str() == "chat.rooms" => {
                    let Ok(mut result) = serde_json::from_value::<ChatContextResult>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    if let Some((target, call_id)) = pending_context.remove(&result.call_id) {
                        result.call_id = call_id;
                        self.send_context_result(&target, result).await;
                    }
                }
                _ => {}
            }
        }
    }

    async fn send_result(&self, target: &ResidentKey, result: RoomMembersResult) {
        let flow = FlowMessage::new(
            MessageKind::new(ROOM_MEMBERS_TOOL_RESULT_KIND).expect("static kind"),
            serde_json::to_value(result).expect("serializable result"),
        );
        let instance_id = *self.instance_id.get().expect("registered");
        if let Err(error) = self.messages.send(instance_id, target, flow).await {
            eprintln!("Room tools could not reply to {target}: {error}");
        }
    }

    async fn send_context_result(&self, target: &ResidentKey, result: ChatContextResult) {
        let flow = FlowMessage::new(
            MessageKind::new(CHAT_CONTEXT_TOOL_RESULT_KIND).expect("static kind"),
            serde_json::to_value(result).expect("serializable context result"),
        );
        let instance_id = *self.instance_id.get().expect("registered");
        if let Err(error) = self.messages.send(instance_id, target, flow).await {
            eprintln!("Room tools could not reply to {target}: {error}");
        }
    }
}

pub struct RoomToolsResidentRuntime {
    resident: Arc<RoomToolsResident>,
    worker: JoinHandle<()>,
}

impl std::fmt::Debug for RoomToolsResidentRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoomToolsResidentRuntime")
            .finish_non_exhaustive()
    }
}

impl RoomToolsResidentRuntime {
    pub async fn shutdown(self) -> Result<(), RegistrationError> {
        let instance_id = *self.resident.instance_id.get().expect("registered");
        self.resident.registration.unregister(instance_id).await?;
        self.worker.abort();
        Ok(())
    }
}
