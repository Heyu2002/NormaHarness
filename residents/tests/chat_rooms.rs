use base64::{Engine, engine::general_purpose::STANDARD};
use image::{DynamicImage, ImageFormat};
use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use norma_harness::{
    CapabilityKey, FlowMessage, Mailbox, MailboxAddress, MailboxError, MessageKind, NormaHarness,
    Resident, ResidentDescriptor, ResidentEvent, ResidentInstanceId, ResidentKey,
};
use norma_residents::{
    chat::{ChatResident, ChatRoom, storage::ChatStorage},
    llm::{
        LlmChatTurnKind, LlmRoomKind, LlmTurnRequest, LlmTurnResult, LlmTurnStatus,
        TURN_REQUEST_KIND, TURN_RESULT_KIND,
    },
    media::MediaStore,
    tools::{
        CHAT_CONTEXT_TOOL_REQUEST_KIND, CHAT_CONTEXT_TOOL_RESULT_KIND, ChatContextResult,
        ChatContextToolRequest, MEDIA_PUBLISH_REQUEST_KIND, MEDIA_PUBLISH_RESULT_KIND,
        MediaPublishRequest, MediaPublishResult, ROOM_MEMBERS_TOOL_REQUEST_KIND,
        ROOM_MEMBERS_TOOL_RESULT_KIND, RoomMembersResult, RoomMembersToolRequest,
        RoomToolsResident, TOOL_CATALOG_REQUEST_KIND, TOOL_CATALOG_RESULT_KIND, ToolCatalogRequest,
        ToolCatalogResult,
    },
};
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::{
    sync::{Barrier, Mutex, mpsc},
    task::JoinHandle,
    time::sleep,
};

struct FakeMailbox(mpsc::UnboundedSender<ResidentEvent>);

impl Mailbox for FakeMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        self.0
            .send(event)
            .map_err(|error| MailboxError::new(error.to_string()))
    }
}

struct FakeLlm {
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
}

impl Resident for FakeLlm {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }
    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }
}

type Log = Arc<Mutex<Vec<(String, LlmTurnRequest)>>>;

async fn receive_kind<T: DeserializeOwned>(
    inbox: &mut mpsc::UnboundedReceiver<ResidentEvent>,
    kind: &str,
) -> Result<T, Box<dyn Error>> {
    while let Some(event) = inbox.recv().await {
        if let ResidentEvent::Message(message) = event {
            if message.message().kind().as_str() == kind {
                return Ok(serde_json::from_value(message.message().payload().clone())?);
            }
        }
    }
    Err(format!("inbox closed before {kind}").into())
}

async fn start_fake(
    harness: &NormaHarness,
    name: &str,
    log: Log,
) -> Result<(ResidentInstanceId, JoinHandle<()>), Box<dyn Error>> {
    start_fake_with_barrier(harness, name, log, None).await
}

async fn start_fake_with_barrier(
    harness: &NormaHarness,
    name: &str,
    log: Log,
    group_barrier: Option<Arc<Barrier>>,
) -> Result<(ResidentInstanceId, JoinHandle<()>), Box<dyn Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let fake = Arc::new(FakeLlm {
        descriptor: ResidentDescriptor::new(ResidentKey::new(name)?, [CapabilityKey::new("llm")?]),
        mailbox: Arc::new(FakeMailbox(tx)),
    });
    let id = harness
        .registration_sender()
        .register(fake)
        .await?
        .instance_id();
    let name = name.to_owned();
    let messages = harness.message_sender();
    let worker = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let ResidentEvent::Message(message) = event else {
                continue;
            };
            if message.message().kind().as_str() != TURN_REQUEST_KIND {
                continue;
            }
            let request: LlmTurnRequest =
                serde_json::from_value(message.message().payload().clone()).unwrap();
            log.lock().await.push((name.clone(), request.clone()));
            if request
                .prompt
                .starts_with("你正在一个多 Agent 协作聊天室中。用户任务：\nDesign X")
            {
                if let Some(barrier) = &group_barrier {
                    barrier.wait().await;
                }
            }
            if name == "alpha" {
                sleep(Duration::from_millis(25)).await;
            }
            let result = LlmTurnResult {
                request_id: request.request_id.clone(),
                thread_id: Some(format!("{name}-thread")),
                turn_id: Some(request.request_id),
                status: LlmTurnStatus::Completed,
                final_response: Some(if request.prompt.contains("Route mentions") {
                    if name == "alpha" {
                        "@beta 请接着回答。".into()
                    } else {
                        "beta-answer".into()
                    }
                } else if request.chat_turn_kind == Some(LlmChatTurnKind::Mention) {
                    "@你 beta 的完整答案。".into()
                } else {
                    format!("{name}-answer")
                }),
                error: None,
                compacted: false,
                attachments: Vec::new(),
            };
            messages
                .send(
                    id,
                    message.source(),
                    FlowMessage::new(
                        MessageKind::new(TURN_RESULT_KIND).unwrap(),
                        serde_json::to_value(result).unwrap(),
                    ),
                )
                .await
                .unwrap();
        }
    });
    Ok((id, worker))
}

async fn wait_done(chat: &Arc<ChatResident>, id: u64) -> ChatRoom {
    loop {
        let room = chat.room(id).await.unwrap();
        if !room.busy {
            return room;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn local_history_survives_sleep_and_incognito_stays_out_of_snapshot()
-> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let runtime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let chat = runtime.resident();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let (llm_id, worker) = start_fake(&harness, "alpha", log).await?;
    let path = std::env::temp_dir().join(format!(
        "norma-chat-test-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    chat.enable_storage(ChatStorage::new(path.clone())?).await?;
    let normal = chat
        .ensure_solo_room(ResidentKey::new("alpha")?, "Alpha".into())
        .await?;
    chat.heartbeat(normal.id, "viewer").await?;
    chat.send_user_message(normal.id, "remember this".into())
        .await?;
    wait_done(&chat, normal.id).await;
    chat.leave(normal.id, "viewer").await;
    let slept = chat.sleep_due(u64::MAX, 45_000, 0).await;
    assert_eq!(slept.len(), 1);
    assert_eq!(slept[0].id, normal.id);
    assert!(chat.sleep_due(u64::MAX, 45_000, 0).await.is_empty());
    chat.send_user_message(normal.id, "next segment".into())
        .await?;
    let resumed = wait_done(&chat, normal.id).await;
    assert_eq!(resumed.id, normal.id);
    assert!(!resumed.sleeping);
    assert_eq!(resumed.messages.len(), 4);
    let (first_private, second_private) = tokio::join!(
        chat.ensure_incognito_solo_room(ResidentKey::new("alpha")?, "private".into()),
        chat.ensure_incognito_solo_room(ResidentKey::new("alpha")?, "private".into()),
    );
    assert_eq!(first_private?.id, second_private?.id);
    assert_eq!(chat.list_rooms().await.len(), 2);
    let snapshot = ChatStorage::new(path.clone())?.load()?.unwrap();
    assert_eq!(snapshot.rooms.len(), 1);
    assert_eq!(snapshot.rooms[0].messages.len(), 4);
    chat.set_archived(normal.id, true).await?;
    let fresh = chat
        .ensure_solo_room(ResidentKey::new("alpha")?, "Alpha".into())
        .await?;
    assert_ne!(fresh.id, normal.id);
    harness.registration_sender().unregister(llm_id).await?;
    worker.abort();
    runtime.shutdown().await?;
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("json.bak"));
    Ok(())
}

#[tokio::test]
async fn solo_and_group_route_through_llm_protocol() -> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let chat_runtime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let chat = chat_runtime.resident();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let group_barrier = Arc::new(Barrier::new(2));
    let (alpha_id, alpha_worker) =
        start_fake_with_barrier(&harness, "alpha", log.clone(), Some(group_barrier.clone()))
            .await?;
    let (beta_id, beta_worker) =
        start_fake_with_barrier(&harness, "beta", log.clone(), Some(group_barrier)).await?;

    let (first, second) = tokio::join!(
        chat.ensure_solo_room(ResidentKey::new("alpha")?, "Alpha".into()),
        chat.ensure_solo_room(ResidentKey::new("alpha")?, "Alpha".into()),
    );
    let solo = first?;
    assert_eq!(
        solo.id, second?.id,
        "direct chat reuses one room per Resident"
    );
    chat.send_user_message(solo.id, "Handle solo".into())
        .await?;
    let done = wait_done(&chat, solo.id).await;
    assert_eq!(done.messages.len(), 2);
    assert_eq!(done.messages[1].role, "agent");
    assert_eq!(log.lock().await.len(), 1);

    let group = chat
        .create_room(
            "Group".into(),
            vec![ResidentKey::new("alpha")?, ResidentKey::new("beta")?],
        )
        .await?;
    chat.send_user_message(group.id, "Design X".into()).await?;
    let done = tokio::time::timeout(Duration::from_secs(2), wait_done(&chat, group.id)).await?;
    assert_eq!(
        done.messages
            .iter()
            .map(|message| message.role.as_str())
            .collect::<Vec<_>>(),
        vec!["user", "agent", "agent"]
    );
    let calls = log.lock().await.clone();
    assert_eq!(calls.len(), 3);
    let initial = &calls[1..3];
    let alpha = &initial.iter().find(|(name, _)| name == "alpha").unwrap().1;
    let beta = &initial.iter().find(|(name, _)| name == "beta").unwrap().1;
    assert!(!alpha.prompt.contains("beta-answer"));
    assert!(!beta.prompt.contains("alpha-answer"));
    assert!(initial.iter().all(|(_, request)| {
        request
            .context
            .iter()
            .all(|event| event.room_id != group.id || event.role != "agent")
    }));
    assert!(calls.iter().all(|(_, request)| request.thread_id.is_none()));
    assert!(alpha.context.iter().any(|event| {
        event.room_id == solo.id && event.role == "user" && event.text == "Handle solo"
    }));
    assert!(beta.context.iter().all(|event| event.room_id != solo.id));
    chat.send_user_message(group.id, "Follow up".into()).await?;
    wait_done(&chat, group.id).await;
    let calls = log.lock().await.clone();
    let follow_up = &calls[3..5];
    let alpha = &follow_up
        .iter()
        .find(|(name, _)| name == "alpha")
        .unwrap()
        .1;
    let beta = &follow_up.iter().find(|(name, _)| name == "beta").unwrap().1;
    assert!(alpha.context.iter().any(|event| {
        event.room_id == solo.id && event.role == "user" && event.text == "Handle solo"
    }));
    assert!(beta.context.iter().all(|event| event.room_id == group.id));

    chat.send_user_message(group.id, "@beta Please answer this".into())
        .await?;
    let done = wait_done(&chat, group.id).await;
    assert_eq!(done.messages.last().unwrap().author, "beta");
    let calls = log.lock().await.clone();
    assert_eq!(calls.len(), 6, "a mention calls only the named Resident");
    assert_eq!(calls[5].0, "beta");
    let origin = calls[5].1.origin.as_ref().expect("group origin");
    assert_eq!(origin.kind, LlmRoomKind::Group);
    assert_eq!(origin.room_id, group.id);
    assert_eq!(origin.room_name, "Group");
    assert_eq!(
        calls[0].1.origin.as_ref().expect("solo origin").kind,
        LlmRoomKind::Solo
    );

    chat.send_user_message(group.id, "@{alpha} Your view?".into())
        .await?;
    wait_done(&chat, group.id).await;
    let calls = log.lock().await.clone();
    assert_eq!(calls.len(), 7);
    assert_eq!(calls[6].0, "alpha");
    assert!(calls[6].1.context.iter().any(|event| {
        event.room_id == group.id
            && event.role == "user"
            && event.text == "@beta Please answer this"
    }));
    assert!(calls[6].1.context.iter().any(|event| {
        event.room_id == group.id && event.author == "beta" && event.text == "beta-answer"
    }));
    assert!(
        chat.send_user_message(group.id, "@outsider Please answer".into())
            .await
            .is_err()
    );

    chat_runtime.shutdown().await?;
    harness.registration_sender().unregister(alpha_id).await?;
    harness.registration_sender().unregister(beta_id).await?;
    alpha_worker.abort();
    beta_worker.abort();
    Ok(())
}

#[tokio::test]
async fn group_member_mentions_trigger_a_follow_up_and_can_answer_user()
-> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let chat_runtime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let chat = chat_runtime.resident();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let (alpha_id, alpha_worker) = start_fake(&harness, "alpha", log.clone()).await?;
    let (beta_id, beta_worker) = start_fake(&harness, "beta", log.clone()).await?;
    let group = chat
        .create_room(
            "Group".into(),
            vec![ResidentKey::new("alpha")?, ResidentKey::new("beta")?],
        )
        .await?;

    chat.send_user_message(group.id, "@alpha Route mentions".into())
        .await?;
    let done = tokio::time::timeout(Duration::from_secs(2), wait_done(&chat, group.id)).await?;
    assert_eq!(
        done.messages
            .iter()
            .map(|message| (message.role.as_str(), message.author.as_str()))
            .collect::<Vec<_>>(),
        vec![("user", "你"), ("agent", "alpha"), ("summary", "beta")]
    );
    let calls = log.lock().await.clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, "alpha");
    assert_eq!(calls[1].0, "beta");
    assert_eq!(calls[1].1.chat_turn_kind, Some(LlmChatTurnKind::Mention));
    assert!(calls[1].1.context.iter().any(|message| {
        message.room_id == group.id
            && message.author == "alpha"
            && message.text == "@beta 请接着回答。"
    }));

    chat_runtime.shutdown().await?;
    harness.registration_sender().unregister(alpha_id).await?;
    harness.registration_sender().unregister(beta_id).await?;
    alpha_worker.abort();
    beta_worker.abort();
    Ok(())
}

#[tokio::test]
async fn room_tools_query_members_through_rtdf() -> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let chat_runtime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let tools_runtime =
        RoomToolsResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let (alpha_tx, mut alpha_rx) = mpsc::unbounded_channel();
    let alpha = Arc::new(FakeLlm {
        descriptor: ResidentDescriptor::new(
            ResidentKey::new("alpha")?,
            [CapabilityKey::new("llm")?],
        ),
        mailbox: Arc::new(FakeMailbox(alpha_tx)),
    });
    let alpha_id = harness
        .registration_sender()
        .register(alpha)
        .await?
        .instance_id();
    let (beta_tx, _beta_rx) = mpsc::unbounded_channel();
    let beta = Arc::new(FakeLlm {
        descriptor: ResidentDescriptor::new(
            ResidentKey::new("beta")?,
            [CapabilityKey::new("llm")?],
        ),
        mailbox: Arc::new(FakeMailbox(beta_tx)),
    });
    let beta_id = harness
        .registration_sender()
        .register(beta)
        .await?
        .instance_id();
    let group = chat_runtime
        .resident()
        .create_room(
            "Architecture".into(),
            vec![ResidentKey::new("alpha")?, ResidentKey::new("beta")?],
        )
        .await?;
    let request = RoomMembersToolRequest {
        call_id: "call-1".into(),
        room_id: group.id,
    };
    harness
        .message_sender()
        .send(
            alpha_id,
            &ResidentKey::new("tools.rooms")?,
            FlowMessage::new(
                MessageKind::new(ROOM_MEMBERS_TOOL_REQUEST_KIND)?,
                serde_json::to_value(request)?,
            ),
        )
        .await?;
    let result = loop {
        let Some(event) = alpha_rx.recv().await else {
            return Err("room-members caller inbox closed".into());
        };
        if let ResidentEvent::Message(message) = event {
            if message.message().kind().as_str() == ROOM_MEMBERS_TOOL_RESULT_KIND {
                break serde_json::from_value::<RoomMembersResult>(
                    message.message().payload().clone(),
                )?;
            }
        }
    };
    assert_eq!(result.room_name.as_deref(), Some("Architecture"));
    assert_eq!(result.members, vec!["alpha", "beta"]);
    assert!(result.error.is_none());
    tools_runtime.shutdown().await?;
    chat_runtime.shutdown().await?;
    harness.registration_sender().unregister(alpha_id).await?;
    harness.registration_sender().unregister(beta_id).await?;
    Ok(())
}

#[tokio::test]
async fn tool_catalog_and_chat_context_respect_membership_incognito_and_snapshot()
-> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let chat_runtime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let tools_runtime =
        RoomToolsResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let (alpha_tx, mut alpha_rx) = mpsc::unbounded_channel();
    let alpha = Arc::new(FakeLlm {
        descriptor: ResidentDescriptor::new(
            ResidentKey::new("alpha")?,
            [CapabilityKey::new("llm")?],
        ),
        mailbox: Arc::new(FakeMailbox(alpha_tx)),
    });
    let alpha_id = harness
        .registration_sender()
        .register(alpha)
        .await?
        .instance_id();
    let (beta_tx, mut beta_rx) = mpsc::unbounded_channel();
    let beta = Arc::new(FakeLlm {
        descriptor: ResidentDescriptor::new(
            ResidentKey::new("beta")?,
            [CapabilityKey::new("llm")?],
        ),
        mailbox: Arc::new(FakeMailbox(beta_tx)),
    });
    let beta_id = harness
        .registration_sender()
        .register(beta)
        .await?
        .instance_id();
    let normal = chat_runtime
        .resident()
        .create_room("normal".into(), vec![ResidentKey::new("alpha")?])
        .await?;
    let private = chat_runtime
        .resident()
        .create_room_with_mode("private".into(), vec![ResidentKey::new("alpha")?], true)
        .await?;
    let shared = chat_runtime
        .resident()
        .create_room(
            "shared".into(),
            vec![ResidentKey::new("alpha")?, ResidentKey::new("beta")?],
        )
        .await?;
    let mut rooms = vec![normal.clone(), private.clone(), shared.clone()];
    for (room, messages) in rooms.iter_mut().zip([
        json!([
            {"id": 1, "role": "user", "author": "你", "text": "normal user", "created_at": 1},
            {"id": 2, "role": "agent", "author": "alpha", "text": "normal answer", "created_at": 2}
        ]),
        json!([
            {"id": 3, "role": "user", "author": "你", "text": "private user", "created_at": 3},
            {"id": 4, "role": "agent", "author": "alpha", "text": "private answer", "created_at": 4}
        ]),
        json!([
            {"id": 5, "role": "user", "author": "你", "text": "shared user", "created_at": 5},
            {"id": 6, "role": "agent", "author": "beta", "text": "shared answer", "created_at": 6}
        ]),
    ]) {
        room.messages = serde_json::from_value(messages)?;
    }
    chat_runtime.resident().restore_rooms(rooms).await;

    harness
        .message_sender()
        .send(
            alpha_id,
            &ResidentKey::new("tools.rooms")?,
            FlowMessage::new(
                MessageKind::new(TOOL_CATALOG_REQUEST_KIND)?,
                serde_json::to_value(ToolCatalogRequest {
                    request_id: "catalog-1".into(),
                })?,
            ),
        )
        .await?;
    let catalog: ToolCatalogResult = receive_kind(&mut alpha_rx, TOOL_CATALOG_RESULT_KIND).await?;
    assert!(
        catalog
            .tools
            .iter()
            .any(|tool| tool["name"] == "read_chat_context")
    );
    assert!(
        catalog
            .tools
            .iter()
            .any(|tool| tool["name"] == "list_group_members")
    );

    let request = ChatContextToolRequest {
        call_id: "context-1".into(),
        room_id: normal.id,
        visible_through: HashMap::from([(normal.id, 2), (shared.id, 5), (private.id, 4)]),
        scope_all: true,
        before_message_id: None,
        limit: 50,
    };
    harness
        .message_sender()
        .send(
            alpha_id,
            &ResidentKey::new("tools.rooms")?,
            FlowMessage::new(
                MessageKind::new(CHAT_CONTEXT_TOOL_REQUEST_KIND)?,
                serde_json::to_value(&request)?,
            ),
        )
        .await?;
    let result: ChatContextResult =
        receive_kind(&mut alpha_rx, CHAT_CONTEXT_TOOL_RESULT_KIND).await?;
    assert!(result.error.is_none(), "{result:?}");
    assert_eq!(
        result
            .messages
            .iter()
            .map(|event| event.id)
            .collect::<Vec<_>>(),
        vec![1, 2, 5]
    );

    let mut current_only = request.clone();
    current_only.call_id = "context-current".into();
    current_only.scope_all = false;
    current_only.limit = 1;
    harness
        .message_sender()
        .send(
            alpha_id,
            &ResidentKey::new("tools.rooms")?,
            FlowMessage::new(
                MessageKind::new(CHAT_CONTEXT_TOOL_REQUEST_KIND)?,
                serde_json::to_value(&current_only)?,
            ),
        )
        .await?;
    let result: ChatContextResult =
        receive_kind(&mut alpha_rx, CHAT_CONTEXT_TOOL_RESULT_KIND).await?;
    assert_eq!(result.messages[0].id, 2);
    assert!(result.has_earlier);
    assert_eq!(result.next_before_message_id, Some(2));

    let private_request = ChatContextToolRequest {
        call_id: "context-2".into(),
        room_id: private.id,
        visible_through: HashMap::from([(normal.id, 2), (shared.id, 6), (private.id, 4)]),
        scope_all: true,
        before_message_id: None,
        limit: 50,
    };
    harness
        .message_sender()
        .send(
            alpha_id,
            &ResidentKey::new("tools.rooms")?,
            FlowMessage::new(
                MessageKind::new(CHAT_CONTEXT_TOOL_REQUEST_KIND)?,
                serde_json::to_value(&private_request)?,
            ),
        )
        .await?;
    let result: ChatContextResult =
        receive_kind(&mut alpha_rx, CHAT_CONTEXT_TOOL_RESULT_KIND).await?;
    assert_eq!(
        result
            .messages
            .iter()
            .map(|event| event.id)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );

    let mut unauthorized = request;
    unauthorized.call_id = "context-3".into();
    harness
        .message_sender()
        .send(
            beta_id,
            &ResidentKey::new("tools.rooms")?,
            FlowMessage::new(
                MessageKind::new(CHAT_CONTEXT_TOOL_REQUEST_KIND)?,
                serde_json::to_value(unauthorized)?,
            ),
        )
        .await?;
    let result: ChatContextResult =
        receive_kind(&mut beta_rx, CHAT_CONTEXT_TOOL_RESULT_KIND).await?;
    assert!(result.error.is_some());
    assert!(result.messages.is_empty());

    let same_call = ChatContextToolRequest {
        call_id: "same-call-id".into(),
        room_id: shared.id,
        visible_through: HashMap::from([(shared.id, 6)]),
        scope_all: false,
        before_message_id: None,
        limit: 50,
    };
    let sender = harness.message_sender();
    let tools_key = ResidentKey::new("tools.rooms")?;
    let (alpha_send, beta_send) = tokio::join!(
        sender.send(
            alpha_id,
            &tools_key,
            FlowMessage::new(
                MessageKind::new(CHAT_CONTEXT_TOOL_REQUEST_KIND)?,
                serde_json::to_value(&same_call)?,
            ),
        ),
        sender.send(
            beta_id,
            &tools_key,
            FlowMessage::new(
                MessageKind::new(CHAT_CONTEXT_TOOL_REQUEST_KIND)?,
                serde_json::to_value(&same_call)?,
            ),
        ),
    );
    alpha_send?;
    beta_send?;
    let (alpha_result, beta_result) = tokio::join!(
        receive_kind(&mut alpha_rx, CHAT_CONTEXT_TOOL_RESULT_KIND),
        receive_kind(&mut beta_rx, CHAT_CONTEXT_TOOL_RESULT_KIND),
    );
    let alpha_result: ChatContextResult = alpha_result?;
    let beta_result: ChatContextResult = beta_result?;
    assert_eq!(alpha_result.call_id, "same-call-id");
    assert_eq!(beta_result.call_id, "same-call-id");
    assert_eq!(alpha_result.messages.len(), 2);
    assert_eq!(beta_result.messages.len(), 2);

    tools_runtime.shutdown().await?;
    chat_runtime.shutdown().await?;
    harness.registration_sender().unregister(alpha_id).await?;
    harness.registration_sender().unregister(beta_id).await?;
    Ok(())
}

#[tokio::test]
async fn tool_resident_publishes_a_gif_attachment() -> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let media = Arc::new(MediaStore::create()?);
    let tools_runtime = RoomToolsResident::launch_with_media(
        harness.registration_sender(),
        harness.message_sender(),
        media.clone(),
    )
    .await?;
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let caller = Arc::new(FakeLlm {
        descriptor: ResidentDescriptor::new(
            ResidentKey::new("alpha")?,
            [CapabilityKey::new("llm")?],
        ),
        mailbox: Arc::new(FakeMailbox(sender)),
    });
    let caller_id = harness
        .registration_sender()
        .register(caller)
        .await?
        .instance_id();
    let mut encoded = std::io::Cursor::new(Vec::new());
    DynamicImage::new_rgba8(2, 2).write_to(&mut encoded, ImageFormat::Gif)?;
    let original = encoded.into_inner();
    let request = MediaPublishRequest {
        call_id: "gif-1".into(),
        filename: "animation.gif".into(),
        mime_type: "image/gif".into(),
        base64: STANDARD.encode(&original),
        incognito: false,
    };
    harness
        .message_sender()
        .send(
            caller_id,
            &ResidentKey::new("tools.rooms")?,
            FlowMessage::new(
                MessageKind::new(MEDIA_PUBLISH_REQUEST_KIND)?,
                serde_json::to_value(request)?,
            ),
        )
        .await?;
    let result: MediaPublishResult = loop {
        let Some(event) = receiver.recv().await else {
            return Err("media caller inbox closed".into());
        };
        if let ResidentEvent::Message(message) = event {
            if message.message().kind().as_str() == MEDIA_PUBLISH_RESULT_KIND {
                break serde_json::from_value(message.message().payload().clone())?;
            }
        }
    };
    assert!(result.error.is_none());
    let asset = result.asset.expect("published GIF");
    assert_eq!(asset.mime_type, "image/gif");
    assert_eq!(std::fs::read(&asset.path)?, original);
    assert!(
        asset.model_paths[0]
            .to_string_lossy()
            .ends_with("first-frame.png")
    );
    media.remove(&asset.id);
    tools_runtime.shutdown().await?;
    harness.registration_sender().unregister(caller_id).await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_rooms_serialize_one_residents_memory() -> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let chat_runtime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let chat = chat_runtime.resident();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let (alpha_id, alpha_worker) = start_fake(&harness, "alpha", log.clone()).await?;
    let (beta_id, beta_worker) = start_fake(&harness, "beta", log.clone()).await?;
    let solo = chat
        .ensure_solo_room(ResidentKey::new("alpha")?, "Alpha".into())
        .await?;
    let group = chat
        .create_room(
            "Group".into(),
            vec![ResidentKey::new("alpha")?, ResidentKey::new("beta")?],
        )
        .await?;
    let (solo_sent, group_sent) = tokio::join!(
        chat.send_user_message(solo.id, "solo task".into()),
        chat.send_user_message(group.id, "group task".into()),
    );
    solo_sent?;
    group_sent?;
    let (_, _) = tokio::join!(wait_done(&chat, solo.id), wait_done(&chat, group.id));
    let calls = log.lock().await.clone();
    let alpha_calls = calls
        .iter()
        .filter(|(name, _)| name == "alpha")
        .map(|(_, request)| request)
        .collect::<Vec<_>>();
    assert_eq!(alpha_calls.len(), 2);
    for (index, request) in alpha_calls.iter().enumerate() {
        let prior_answers = request
            .context
            .iter()
            .filter(|event| event.author == "alpha" && event.text == "alpha-answer")
            .count();
        assert_eq!(prior_answers, index);
    }
    chat_runtime.shutdown().await?;
    harness.registration_sender().unregister(alpha_id).await?;
    harness.registration_sender().unregister(beta_id).await?;
    alpha_worker.abort();
    beta_worker.abort();
    Ok(())
}

/// Run manually with `cargo test -p norma-residents --test chat_rooms live_codex_can_call_group_member_tool -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "requires a locally authenticated Codex CLI and a live model turn"]
async fn live_codex_can_call_group_member_tool() -> Result<(), Box<dyn Error>> {
    use norma_residents::codex::{CodexResident, CodexResidentConfig};

    let harness = NormaHarness::new();
    let chat_runtime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let tools_runtime =
        RoomToolsResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    let (beta_tx, _beta_rx) = mpsc::unbounded_channel();
    let beta = Arc::new(FakeLlm {
        descriptor: ResidentDescriptor::new(
            ResidentKey::new("beta")?,
            [CapabilityKey::new("llm")?],
        ),
        mailbox: Arc::new(FakeMailbox(beta_tx)),
    });
    let beta_id = harness
        .registration_sender()
        .register(beta)
        .await?
        .instance_id();
    let mut config = CodexResidentConfig::new(ResidentKey::new("codex")?, std::env::current_dir()?);
    config.model = Some("gpt-5.6-luna".into());
    config.effort = Some("low".into());
    let codex_runtime = CodexResident::launch(
        config,
        harness.registration_sender(),
        harness.message_sender(),
    )
    .await?;
    let group = chat_runtime
        .resident()
        .create_room(
            "Tool smoke group".into(),
            vec![ResidentKey::new("codex")?, ResidentKey::new("beta")?],
        )
        .await?;
    let mut restored = group.clone();
    restored.messages = serde_json::from_value(json!([{
        "id": 1,
        "role": "user",
        "author": "你",
        "text": "历史标记：蓝色方块",
        "created_at": 1
    }]))?;
    chat_runtime.resident().restore_rooms(vec![restored]).await;
    chat_runtime
        .resident()
        .send_user_message(
            group.id,
            "@codex 请调用 read_chat_context 工具读取群聊历史，再调用 list_group_members 工具。仅输出历史标记和工具返回的成员名称。"
                .into(),
        )
        .await?;
    let room = tokio::time::timeout(Duration::from_secs(240), async {
        loop {
            let room = chat_runtime
                .resident()
                .room(group.id)
                .await
                .expect("group exists");
            if !room.busy {
                break room;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    let room = match room {
        Ok(room) => room,
        Err(error) => {
            eprintln!(
                "live room at timeout: {:?}",
                chat_runtime.resident().room(group.id).await
            );
            return Err(error.into());
        }
    };
    let answer = room.messages.last().expect("Codex answer");
    assert_eq!(answer.role, "agent", "{answer:?}");
    assert!(
        answer.text.contains("蓝色方块")
            && answer.text.contains("codex")
            && answer.text.contains("beta"),
        "{answer:?}"
    );
    codex_runtime.shutdown().await?;
    tools_runtime.shutdown().await?;
    chat_runtime.shutdown().await?;
    harness.registration_sender().unregister(beta_id).await?;
    Ok(())
}
