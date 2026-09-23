//! Read-only tools exposed to LLM Residents through RTDF.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use crate::media::{MediaAsset, MediaStore};
use norma_harness::{
    CapabilityKey, FlowMessage, Mailbox, MailboxAddress, MailboxError, MessageKind, MessageSender,
    RegistrationError, RegistrationSender, Resident, ResidentDescriptor, ResidentEvent,
    ResidentInstanceId, ResidentKey,
};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle};

pub const TOOL_RESIDENT_KEY: &str = "tools.rooms";
pub const TOOL_CAPABILITY: &str = "tool.room_members";
pub const ROOM_MEMBERS_TOOL_REQUEST_KIND: &str = "tool.room_members.request";
pub const ROOM_MEMBERS_TOOL_RESULT_KIND: &str = "tool.room_members.result";
pub(crate) const ROOM_MEMBERS_QUERY_KIND: &str = "chat.room_members.query";
pub(crate) const ROOM_MEMBERS_RESULT_KIND: &str = "chat.room_members.result";
pub const MEDIA_PUBLISH_REQUEST_KIND: &str = "tool.media.publish.request";
pub const MEDIA_PUBLISH_RESULT_KIND: &str = "tool.media.publish.result";

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
        let mut pending: HashMap<String, ResidentKey> = HashMap::new();
        while let Some(event) = inbox.recv().await {
            let ResidentEvent::Message(message) = event else {
                continue;
            };
            match message.message().kind().as_str() {
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
                    if request.call_id.trim().is_empty()
                        || request.room_id == 0
                        || pending.contains_key(&request.call_id)
                    {
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
                    pending.insert(request.call_id.clone(), message.source().clone());
                    let query = RoomMembersQuery {
                        call_id: request.call_id.clone(),
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
                        pending.remove(&request.call_id);
                        self.send_result(
                            message.source(),
                            RoomMembersResult::failed(&request, error.to_string()),
                        )
                        .await;
                    }
                }
                ROOM_MEMBERS_RESULT_KIND if message.source().as_str() == "chat.rooms" => {
                    let Ok(result) = serde_json::from_value::<RoomMembersResult>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    if let Some(target) = pending.remove(&result.call_id) {
                        self.send_result(&target, result).await;
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
