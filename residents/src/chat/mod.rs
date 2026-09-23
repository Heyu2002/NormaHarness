//! A room coordinator Resident. It sends turns through RTDF and correlates
//! replies without giving the web layer direct access to another Resident.

pub mod storage;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use norma_harness::{
    Mailbox, MailboxAddress, MailboxError, MessageSender, RegistrationError, RegistrationSender,
    Resident, ResidentDescriptor, ResidentEvent, ResidentInstanceId, ResidentKey,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Mutex, broadcast, mpsc, oneshot},
    task::JoinHandle,
};

use self::storage::ChatStorage;
use crate::llm::{
    LlmContextMessage, LlmOrigin, LlmRoomKind, LlmTurnRequest, LlmTurnResult, LlmTurnStatus,
    MEMORY_TRIGGER_KIND, MemoryTrigger, MemoryTriggerReason, TURN_RESULT_KIND,
    turn_request_message,
};
use crate::media::{MAX_MESSAGE_MEDIA, MediaAsset, MediaView};
use crate::tools::{ROOM_MEMBERS_QUERY_KIND, RoomMembersQuery, RoomMembersResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: u64,
    pub role: String,
    pub author: String,
    pub text: String,
    pub created_at: u64,
    #[serde(default)]
    pub attachments: Vec<MediaView>,
    #[serde(skip)]
    pub(crate) media: Vec<MediaAsset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRoom {
    pub id: u64,
    pub name: String,
    pub members: Vec<String>,
    pub messages: Vec<ChatMessage>,
    pub busy: bool,
    pub active_member: Option<String>,
    #[serde(default)]
    pub sleeping: bool,
    #[serde(default)]
    pub sleep_generation: u64,
    #[serde(default)]
    pub last_activity_ms: u64,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub incognito: bool,
}

#[derive(Debug, Error)]
pub enum ChatError {
    #[error("{0}")]
    Invalid(String),
    #[error("room not found")]
    NotFound,
    #[error("this room is already processing a message")]
    Busy,
    #[error("{0}")]
    Internal(String),
}

struct RoomState {
    room: ChatRoom,
    updates: broadcast::Sender<()>,
}

struct PendingTurn {
    expected_source: ResidentKey,
    reply: oneshot::Sender<LlmTurnResult>,
}

#[async_trait]
pub trait ThreadEndHook: Send + Sync {
    async fn run(&self, room: &ChatRoom) -> Result<(), String>;
}

struct ChatMailbox(mpsc::UnboundedSender<ResidentEvent>);

impl Mailbox for ChatMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        self.0
            .send(event)
            .map_err(|error| MailboxError::new(error.to_string()))
    }
}

/// Registered coordinator for room state and LLM request/reply correlation.
pub struct ChatResident {
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
    registration: RegistrationSender,
    messages: MessageSender,
    instance_id: OnceLock<ResidentInstanceId>,
    rooms: Mutex<BTreeMap<u64, RoomState>>,
    pending: Mutex<HashMap<String, PendingTurn>>,
    member_turns: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    next_room: AtomicU64,
    next_message: AtomicU64,
    next_request: AtomicU64,
    storage: OnceLock<ChatStorage>,
    presence: Mutex<HashMap<u64, HashMap<String, u64>>>,
    last_presence: Mutex<HashMap<u64, u64>>,
    end_hook: OnceLock<Arc<dyn ThreadEndHook>>,
}

impl std::fmt::Debug for ChatResident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatResident")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl Resident for ChatResident {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }
    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }
}

impl ChatResident {
    pub fn set_end_hook(&self, hook: Arc<dyn ThreadEndHook>) -> Result<(), &'static str> {
        self.end_hook
            .set(hook)
            .map_err(|_| "thread end hook is already configured")
    }

    pub async fn after_sleep(&self, room: ChatRoom) {
        if let Some(hook) = self.end_hook.get() {
            if let Err(error) = hook.run(&room).await {
                eprintln!("thread {} end hook failed: {error}", room.id);
            }
        }
        for member in &room.members {
            self.emit_memory_trigger(&room, member, MemoryTriggerReason::Sleep)
                .await;
        }
    }

    /// Retry memory extraction for rooms restored in the sleeping state.
    /// The memory cursor prevents duplicate extraction after a clean shutdown.
    pub async fn retry_sleep_memory(&self) {
        let rooms = self
            .rooms
            .lock()
            .await
            .values()
            .filter(|state| state.room.sleeping && !state.room.incognito)
            .map(|state| state.room.clone())
            .collect::<Vec<_>>();
        for room in rooms {
            for member in &room.members {
                self.emit_memory_trigger(&room, member, MemoryTriggerReason::Sleep)
                    .await;
            }
        }
    }

    async fn emit_memory_trigger(
        &self,
        room: &ChatRoom,
        member: &str,
        reason: MemoryTriggerReason,
    ) {
        if room.incognito || room.messages.is_empty() {
            return;
        }
        let Ok(target) = ResidentKey::new(member) else {
            return;
        };
        let context = room
            .messages
            .iter()
            .map(|message| LlmContextMessage {
                id: message.id,
                room_id: room.id,
                room_name: room.name.clone(),
                role: message.role.clone(),
                author: message.author.clone(),
                text: message.text.clone(),
                created_at_ms: message.created_at,
                attachments: message.media.clone(),
            })
            .collect();
        let trigger = MemoryTrigger {
            room_id: room.id,
            reason,
            sleep_generation: room.sleep_generation,
            context,
        };
        let message = norma_harness::FlowMessage::new(
            norma_harness::MessageKind::new(MEMORY_TRIGGER_KIND).expect("static kind"),
            serde_json::to_value(trigger).expect("serializable memory trigger"),
        );
        let instance_id = *self.instance_id.get().expect("registered before use");
        if let Err(error) = self.messages.send(instance_id, &target, message).await {
            eprintln!("could not trigger memory extraction for {member}: {error}");
        }
    }

    pub async fn enable_storage(&self, storage: ChatStorage) -> Result<(), ChatError> {
        self.storage
            .set(storage)
            .map_err(|_| ChatError::Internal("chat storage is already configured".into()))?;
        let rooms = self.rooms.lock().await;
        self.save_locked(&rooms)
    }

    fn save_locked(&self, rooms: &BTreeMap<u64, RoomState>) -> Result<(), ChatError> {
        if let Some(storage) = self.storage.get() {
            storage.save(rooms).map_err(|error| {
                ChatError::Internal(format!("could not save chat history: {error}"))
            })?;
        }
        Ok(())
    }

    pub async fn launch(
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<ChatResidentRuntime, RegistrationError> {
        let (events, mut inbox) = mpsc::unbounded_channel();
        let resident = Arc::new(Self {
            descriptor: ResidentDescriptor::new(
                ResidentKey::new("chat.rooms").expect("static key is valid"),
                [],
            ),
            mailbox: Arc::new(ChatMailbox(events)),
            registration,
            messages,
            instance_id: OnceLock::new(),
            rooms: Mutex::new(BTreeMap::new()),
            pending: Mutex::new(HashMap::new()),
            member_turns: Mutex::new(HashMap::new()),
            next_room: AtomicU64::new(1),
            next_message: AtomicU64::new(1),
            next_request: AtomicU64::new(1),
            storage: OnceLock::new(),
            presence: Mutex::new(HashMap::new()),
            last_presence: Mutex::new(HashMap::new()),
            end_hook: OnceLock::new(),
        });
        let receipt = resident.registration.register(resident.clone()).await?;
        resident
            .instance_id
            .set(receipt.instance_id())
            .expect("registers once");
        let worker_resident = resident.clone();
        let worker = tokio::spawn(async move {
            while let Some(event) = inbox.recv().await {
                let ResidentEvent::Message(message) = event else {
                    continue;
                };
                if message.message().kind().as_str() == ROOM_MEMBERS_QUERY_KIND {
                    let source = message.source().clone();
                    if source.as_str() != "tools.rooms" {
                        continue;
                    }
                    let Ok(query) = serde_json::from_value::<RoomMembersQuery>(
                        message.message().payload().clone(),
                    ) else {
                        continue;
                    };
                    let result = worker_resident.room_members_for(&query).await;
                    let reply = norma_harness::FlowMessage::new(
                        norma_harness::MessageKind::new(crate::tools::ROOM_MEMBERS_RESULT_KIND)
                            .expect("static kind"),
                        serde_json::to_value(result).expect("serializable room members"),
                    );
                    let instance_id = *worker_resident.instance_id.get().expect("registered");
                    let _ = worker_resident
                        .messages
                        .send(instance_id, &source, reply)
                        .await;
                    continue;
                }
                if message.message().kind().as_str() != TURN_RESULT_KIND {
                    continue;
                }
                let Ok(result) =
                    serde_json::from_value::<LlmTurnResult>(message.message().payload().clone())
                else {
                    continue;
                };
                let mut pending = worker_resident.pending.lock().await;
                if pending
                    .get(&result.request_id)
                    .is_some_and(|turn| turn.expected_source == *message.source())
                {
                    if let Some(turn) = pending.remove(&result.request_id) {
                        let _ = turn.reply.send(result);
                    }
                }
            }
        });
        Ok(ChatResidentRuntime { resident, worker })
    }

    async fn room_members_for(&self, query: &RoomMembersQuery) -> RoomMembersResult {
        let rooms = self.rooms.lock().await;
        let room = rooms.get(&query.room_id).map(|state| &state.room);
        match room {
            Some(room) if room.members.len() > 1 && room.members.contains(&query.requester) => {
                RoomMembersResult {
                    call_id: query.call_id.clone(),
                    room_id: query.room_id,
                    room_name: Some(room.name.clone()),
                    members: room.members.clone(),
                    error: None,
                }
            }
            _ => RoomMembersResult {
                call_id: query.call_id.clone(),
                room_id: query.room_id,
                room_name: None,
                members: Vec::new(),
                error: Some("group not found or requester is not a member".into()),
            },
        }
    }

    pub async fn create_room(
        &self,
        name: String,
        members: Vec<ResidentKey>,
    ) -> Result<ChatRoom, ChatError> {
        self.create_room_with_mode(name, members, false).await
    }

    pub async fn create_room_with_mode(
        &self,
        name: String,
        members: Vec<ResidentKey>,
        incognito: bool,
    ) -> Result<ChatRoom, ChatError> {
        let name = valid_room_name(name)?;
        if members.is_empty() || members.len() > 8 {
            return Err(ChatError::Invalid("select 1–8 LLM Residents".into()));
        }
        if members.iter().collect::<HashSet<_>>().len() != members.len() {
            return Err(ChatError::Invalid(
                "each Resident can join only once".into(),
            ));
        }
        let id = self.next_room.fetch_add(1, Ordering::Relaxed);
        let room = ChatRoom {
            id,
            name,
            members: members.into_iter().map(|key| key.to_string()).collect(),
            messages: Vec::new(),
            busy: false,
            active_member: None,
            sleeping: false,
            sleep_generation: 0,
            last_activity_ms: now_ms(),
            archived: false,
            incognito,
        };
        let (updates, _) = broadcast::channel(32);
        let mut rooms = self.rooms.lock().await;
        rooms.insert(
            id,
            RoomState {
                room: room.clone(),
                updates,
            },
        );
        self.save_locked(&rooms)?;
        Ok(room)
    }

    /// One direct room per LLM Resident. The lookup and creation share one lock,
    /// so simultaneous clicks cannot create duplicate one-to-one rooms.
    pub async fn ensure_solo_room(
        &self,
        member: ResidentKey,
        name: String,
    ) -> Result<ChatRoom, ChatError> {
        let name = valid_room_name(name)?;
        let mut rooms = self.rooms.lock().await;
        if let Some(existing) = rooms.values().find(|state| {
            !state.room.incognito
                && !state.room.archived
                && state.room.members.len() == 1
                && state.room.members[0] == member.as_str()
        }) {
            return Ok(existing.room.clone());
        }
        let id = self.next_room.fetch_add(1, Ordering::Relaxed);
        let room = ChatRoom {
            id,
            name,
            members: vec![member.to_string()],
            messages: Vec::new(),
            busy: false,
            active_member: None,
            sleeping: false,
            sleep_generation: 0,
            last_activity_ms: now_ms(),
            archived: false,
            incognito: false,
        };
        let (updates, _) = broadcast::channel(32);
        rooms.insert(
            id,
            RoomState {
                room: room.clone(),
                updates,
            },
        );
        self.save_locked(&rooms)?;
        Ok(room)
    }

    pub async fn list_rooms(&self) -> Vec<ChatRoom> {
        self.rooms
            .lock()
            .await
            .values()
            .map(|state| state.room.clone())
            .collect()
    }

    pub async fn room(&self, id: u64) -> Option<ChatRoom> {
        self.rooms
            .lock()
            .await
            .get(&id)
            .map(|state| state.room.clone())
    }

    pub async fn subscribe(&self, id: u64) -> Result<broadcast::Receiver<()>, ChatError> {
        self.rooms
            .lock()
            .await
            .get(&id)
            .map(|state| state.updates.subscribe())
            .ok_or(ChatError::NotFound)
    }

    pub async fn send_user_message(
        self: &Arc<Self>,
        id: u64,
        text: String,
    ) -> Result<ChatRoom, ChatError> {
        self.send_user_message_with_media(id, text, Vec::new())
            .await
    }

    pub async fn restore_rooms(&self, restored: Vec<ChatRoom>) {
        self.restore_rooms_with_media(restored, &HashMap::new())
            .await;
    }

    pub async fn restore_rooms_with_media(
        &self,
        restored: Vec<ChatRoom>,
        media: &HashMap<String, MediaAsset>,
    ) {
        let mut rooms = self.rooms.lock().await;
        for mut room in restored {
            room.busy = false;
            room.active_member = None;
            room.sleeping = true;
            self.next_room.fetch_max(room.id + 1, Ordering::Relaxed);
            for message in &mut room.messages {
                self.next_message
                    .fetch_max(message.id + 1, Ordering::Relaxed);
                message.media.clear();
                for attachment in &mut message.attachments {
                    if let Some(asset) = media.get(&attachment.id) {
                        *attachment = asset.view();
                        message.media.push(asset.clone());
                    }
                }
            }
            let (updates, _) = broadcast::channel(32);
            rooms.insert(room.id, RoomState { room, updates });
        }
    }

    pub async fn send_user_message_with_media(
        self: &Arc<Self>,
        id: u64,
        text: String,
        media: Vec<MediaAsset>,
    ) -> Result<ChatRoom, ChatError> {
        let text = text.trim().to_owned();
        if (text.is_empty() && media.is_empty()) || text.chars().count() > 20_000 {
            return Err(ChatError::Invalid(
                "message needs text or an image, with at most 20000 characters".into(),
            ));
        }
        if media.len() > MAX_MESSAGE_MEDIA {
            return Err(ChatError::Invalid(format!(
                "at most {MAX_MESSAGE_MEDIA} images per message"
            )));
        }
        let mut rooms = self.rooms.lock().await;
        let state = rooms.get_mut(&id).ok_or(ChatError::NotFound)?;
        if state.room.busy {
            return Err(ChatError::Busy);
        }
        if state.room.archived {
            return Err(ChatError::Invalid(
                "unarchive this room before sending a message".into(),
            ));
        }
        let is_group = state.room.members.len() > 1;
        let mentioned = if is_group {
            mentioned_members(&text, &state.room.members)?
        } else {
            Vec::new()
        };
        let targeted = !mentioned.is_empty();
        let members = if targeted {
            mentioned
        } else {
            state.room.members.clone()
        };
        state.room.busy = true;
        state.room.sleeping = false;
        state.room.last_activity_ms = now_ms();
        state
            .room
            .messages
            .push(self.new_message("user", "你", text.clone(), media));
        let snapshot = state.room.clone();
        let updates = state.updates.clone();
        self.save_locked(&rooms)?;
        let _ = updates.send(());
        drop(rooms);
        let task = if text.is_empty() {
            "请查看附件并回复。".to_owned()
        } else {
            text
        };
        let resident = self.clone();
        tokio::spawn(async move {
            resident
                .run_turn(id, task, members, is_group, targeted)
                .await
        });
        Ok(snapshot)
    }

    async fn run_turn(
        &self,
        id: u64,
        task: String,
        members: Vec<String>,
        is_group: bool,
        targeted: bool,
    ) {
        let mut contributions: Vec<(String, String)> = Vec::new();
        for member in &members {
            self.set_active(id, Some(member.clone())).await;
            let prompt = if is_group {
                contribution_prompt(&task, member, &contributions)
            } else {
                task.clone()
            };
            match self.ask(id, member, prompt, "agent").await {
                Ok(answer) => {
                    contributions.push((member.clone(), answer));
                }
                Err(error) => {
                    self.push_message(id, "error", "系统", format!("{member}: {error}"))
                        .await;
                    self.finish(id).await;
                    return;
                }
            }
        }
        if is_group && !targeted {
            let lead = &members[0];
            self.set_active(id, Some(lead.clone())).await;
            match self
                .ask(id, lead, synthesis_prompt(&task, &contributions), "summary")
                .await
            {
                Ok(_) => {}
                Err(error) => {
                    self.push_message(id, "error", "系统", format!("汇总失败: {error}"))
                        .await
                }
            }
        }
        self.finish(id).await;
    }

    async fn ask(
        &self,
        id: u64,
        member: &str,
        prompt: String,
        response_role: &str,
    ) -> Result<String, ChatError> {
        let target =
            ResidentKey::new(member).map_err(|error| ChatError::Internal(error.to_string()))?;
        // One Resident processes its conversations in a single order. Take the
        // room snapshot only after earlier turns for this Resident finish.
        let member_turn = self
            .member_turns
            .lock()
            .await
            .entry(member.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _turn = member_turn.lock().await;
        let rooms = self.rooms.lock().await;
        let current_room = rooms.get(&id).ok_or(ChatError::NotFound)?;
        let origin = LlmOrigin {
            kind: if current_room.room.members.len() == 1 {
                LlmRoomKind::Solo
            } else {
                LlmRoomKind::Group
            },
            room_id: current_room.room.id,
            room_name: current_room.room.name.clone(),
            incognito: current_room.room.incognito,
        };
        let incognito = current_room.room.incognito;
        let mut context = rooms
            .values()
            .filter(|state| {
                state.room.members.iter().any(|key| key == member)
                    && if incognito {
                        state.room.id == id
                    } else {
                        !state.room.incognito
                    }
            })
            .flat_map(|state| {
                state.room.messages.iter().map(|message| LlmContextMessage {
                    id: message.id,
                    room_id: state.room.id,
                    room_name: state.room.name.clone(),
                    role: message.role.clone(),
                    author: message.author.clone(),
                    text: message.text.clone(),
                    created_at_ms: message.created_at,
                    attachments: message.media.clone(),
                })
            })
            .collect::<Vec<_>>();
        drop(rooms);
        context.sort_by_key(|message| message.id);
        let request_id = format!(
            "chat-{id}-{}",
            self.next_request.fetch_add(1, Ordering::Relaxed)
        );
        let (reply, receiver) = oneshot::channel();
        self.pending.lock().await.insert(
            request_id.clone(),
            PendingTurn {
                expected_source: target.clone(),
                reply,
            },
        );
        let request = LlmTurnRequest {
            request_id: request_id.clone(),
            prompt,
            origin: Some(origin),
            source_resident: None,
            thread_id: None,
            context,
        };
        let instance_id = *self.instance_id.get().expect("registered before use");
        if let Err(error) = self
            .messages
            .send(instance_id, &target, turn_request_message(&request))
            .await
        {
            self.pending.lock().await.remove(&request_id);
            return Err(ChatError::Internal(error.to_string()));
        }
        let result = match receiver.await {
            Ok(result) => result,
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                return Err(ChatError::Internal("Resident reply channel closed".into()));
            }
        };
        if result.status != LlmTurnStatus::Completed {
            return Err(ChatError::Internal(
                result
                    .error
                    .unwrap_or_else(|| "Resident turn failed".into()),
            ));
        }
        let compacted = result.compacted;
        let attachments = result.attachments;
        let answer = result
            .final_response
            .filter(|answer| !answer.trim().is_empty())
            .or_else(|| (!attachments.is_empty()).then(|| "已发送附件".into()))
            .ok_or_else(|| ChatError::Internal("Resident returned an empty response".into()))?;
        self.push_message_with_media(id, response_role, member, answer.clone(), attachments)
            .await;
        if compacted {
            if let Some(room) = self.room(id).await {
                self.emit_memory_trigger(&room, member, MemoryTriggerReason::Compaction)
                    .await;
            }
        }
        Ok(answer)
    }

    fn new_message(
        &self,
        role: &str,
        author: &str,
        text: String,
        media: Vec<MediaAsset>,
    ) -> ChatMessage {
        ChatMessage {
            id: self.next_message.fetch_add(1, Ordering::Relaxed),
            role: role.into(),
            author: author.into(),
            text,
            attachments: media.iter().map(MediaAsset::view).collect(),
            media,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    async fn push_message(&self, id: u64, role: &str, author: &str, text: String) {
        self.push_message_with_media(id, role, author, text, Vec::new())
            .await;
    }

    async fn push_message_with_media(
        &self,
        id: u64,
        role: &str,
        author: &str,
        text: String,
        media: Vec<MediaAsset>,
    ) {
        let mut rooms = self.rooms.lock().await;
        if let Some(state) = rooms.get_mut(&id) {
            state
                .room
                .messages
                .push(self.new_message(role, author, text, media));
            state.room.last_activity_ms = now_ms();
            let updates = state.updates.clone();
            if let Err(error) = self.save_locked(&rooms) {
                eprintln!("{error}");
            }
            let _ = updates.send(());
        }
    }

    async fn set_active(&self, id: u64, member: Option<String>) {
        if let Some(state) = self.rooms.lock().await.get_mut(&id) {
            state.room.active_member = member;
            let _ = state.updates.send(());
        }
    }

    async fn finish(&self, id: u64) {
        let mut rooms = self.rooms.lock().await;
        if let Some(state) = rooms.get_mut(&id) {
            state.room.busy = false;
            state.room.active_member = None;
            state.room.last_activity_ms = now_ms();
            let updates = state.updates.clone();
            if let Err(error) = self.save_locked(&rooms) {
                eprintln!("{error}");
            }
            let _ = updates.send(());
        }
    }

    pub async fn heartbeat(&self, id: u64, client: &str) -> Result<(), ChatError> {
        if client.is_empty() || client.len() > 64 {
            return Err(ChatError::Invalid("invalid client ID".into()));
        }
        if !self.rooms.lock().await.contains_key(&id) {
            return Err(ChatError::NotFound);
        }
        let now = now_ms();
        self.presence
            .lock()
            .await
            .entry(id)
            .or_default()
            .insert(client.to_owned(), now);
        self.last_presence.lock().await.insert(id, now);
        Ok(())
    }

    pub async fn leave(&self, id: u64, client: &str) {
        if let Some(clients) = self.presence.lock().await.get_mut(&id) {
            clients.remove(client);
        }
        self.last_presence.lock().await.insert(id, now_ms());
    }

    /// Sleep a room after its last viewer leaves or after a period without turns.
    /// The room ID and message history remain unchanged on the next wake.
    pub async fn sleep_due(
        &self,
        idle_ms: u64,
        lease_ms: u64,
        leave_grace_ms: u64,
    ) -> Vec<ChatRoom> {
        let now = now_ms();
        let mut rooms = self.rooms.lock().await;
        let mut presence = self.presence.lock().await;
        let last_presence = self.last_presence.lock().await;
        let mut newly_sleeping = Vec::new();
        for (id, state) in rooms.iter_mut() {
            if state.room.sleeping || state.room.busy {
                continue;
            }
            let clients = presence.entry(*id).or_default();
            clients.retain(|_, seen| now.saturating_sub(*seen) < lease_ms);
            let idle = now.saturating_sub(state.room.last_activity_ms) >= idle_ms;
            let left = clients.is_empty()
                && now.saturating_sub(
                    last_presence
                        .get(id)
                        .copied()
                        .unwrap_or(state.room.last_activity_ms),
                ) >= leave_grace_ms;
            if idle || left {
                state.room.sleeping = true;
                state.room.sleep_generation += 1;
                newly_sleeping.push(state.room.clone());
                let _ = state.updates.send(());
            }
        }
        if !newly_sleeping.is_empty() {
            if let Err(error) = self.save_locked(&rooms) {
                eprintln!("{error}");
            }
        }
        newly_sleeping
    }

    pub async fn set_archived(&self, id: u64, archived: bool) -> Result<ChatRoom, ChatError> {
        let mut rooms = self.rooms.lock().await;
        let state = rooms.get_mut(&id).ok_or(ChatError::NotFound)?;
        if state.room.busy {
            return Err(ChatError::Busy);
        }
        if state.room.incognito {
            return Err(ChatError::Invalid(
                "incognito rooms cannot be archived".into(),
            ));
        }
        state.room.archived = archived;
        let room = state.room.clone();
        let updates = state.updates.clone();
        self.save_locked(&rooms)?;
        let _ = updates.send(());
        Ok(room)
    }

    /// Reuse the one in-memory incognito room for this Resident.
    pub async fn ensure_incognito_solo_room(
        &self,
        member: ResidentKey,
        name: String,
    ) -> Result<ChatRoom, ChatError> {
        let name = valid_room_name(name)?;
        let mut rooms = self.rooms.lock().await;
        if let Some(existing) = rooms.values().find(|state| {
            state.room.incognito
                && state.room.members.len() == 1
                && state.room.members[0] == member.as_str()
        }) {
            return Ok(existing.room.clone());
        }
        let id = self.next_room.fetch_add(1, Ordering::Relaxed);
        let room = ChatRoom {
            id,
            name,
            members: vec![member.to_string()],
            messages: Vec::new(),
            busy: false,
            active_member: None,
            sleeping: false,
            sleep_generation: 0,
            last_activity_ms: now_ms(),
            archived: false,
            incognito: true,
        };
        let (updates, _) = broadcast::channel(32);
        rooms.insert(
            id,
            RoomState {
                room: room.clone(),
                updates,
            },
        );
        Ok(room)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn mentioned_members(text: &str, members: &[String]) -> Result<Vec<String>, ChatError> {
    let mut selected = Vec::new();
    for (index, _) in text.match_indices('@') {
        if text[..index]
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            continue;
        }
        let tail = &text[index + 1..];
        let candidate = if let Some(rest) = tail.strip_prefix('{') {
            rest.split_once('}').map(|(key, _)| key)
        } else {
            let end = tail
                .find(|ch: char| !(ch.is_ascii_alphanumeric() || "_.-".contains(ch)))
                .unwrap_or(tail.len());
            let key = tail[..end].trim_end_matches('.');
            (!key.is_empty()).then_some(key)
        };
        let Some(key) = candidate else { continue };
        if !members.iter().any(|member| member == key) {
            return Err(ChatError::Invalid(format!(
                "@{key} is not a member of this group"
            )));
        }
        if !selected.iter().any(|member| member == key) {
            selected.push(key.to_owned());
        }
    }
    Ok(selected)
}

fn valid_room_name(name: String) -> Result<String, ChatError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(ChatError::Invalid(
            "room name must have 1–80 characters".into(),
        ));
    }
    Ok(name.to_owned())
}

fn contribution_prompt(task: &str, member: &str, contributions: &[(String, String)]) -> String {
    let mut prompt = format!(
        "你正在一个多 Agent 协作聊天室中。用户任务：\n{task}\n\n你是 {member}。请给出你的独立分析或可执行结果，清楚说明依据与尚未解决的问题。请勿声称其他成员已经执行了未在下方出现的工作。"
    );
    if !contributions.is_empty() {
        prompt.push_str("\n\n此前成员的贡献：");
        for (name, answer) in contributions {
            prompt.push_str(&format!("\n\n[{name}]\n{answer}"));
        }
        prompt.push_str("\n\n请在此前贡献的基础上补充、纠错或推进任务，避免简单重复。");
    }
    prompt
}

fn synthesis_prompt(task: &str, contributions: &[(String, String)]) -> String {
    let mut prompt = format!(
        "你是协作聊天室的汇总者。用户任务：\n{task}\n\n请综合以下所有成员的实际贡献，给出一致、可直接交付给用户的最终答复；保留有分歧或未验证之处。\n"
    );
    for (name, answer) in contributions {
        prompt.push_str(&format!("\n[{name}]\n{answer}\n"));
    }
    prompt
}

pub struct ChatResidentRuntime {
    resident: Arc<ChatResident>,
    worker: JoinHandle<()>,
}

impl std::fmt::Debug for ChatResidentRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatResidentRuntime")
            .finish_non_exhaustive()
    }
}

impl ChatResidentRuntime {
    pub fn resident(&self) -> Arc<ChatResident> {
        self.resident.clone()
    }
    pub async fn shutdown(self) -> Result<(), RegistrationError> {
        let instance_id = *self.resident.instance_id.get().expect("registered runtime");
        self.resident.registration.unregister(instance_id).await?;
        self.worker.abort();
        Ok(())
    }
}
