use base64::{Engine, engine::general_purpose::STANDARD};
use image::{DynamicImage, ImageFormat};
use std::{error::Error, sync::Arc, time::Duration};

use norma_harness::{
    CapabilityKey, FlowMessage, Mailbox, MailboxAddress, MailboxError, MessageKind, NormaHarness,
    Resident, ResidentDescriptor, ResidentEvent, ResidentInstanceId, ResidentKey,
};
use norma_residents::{
    chat::{ChatResident, ChatRoom, storage::ChatStorage},
    llm::{
        LlmRoomKind, LlmTurnRequest, LlmTurnResult, LlmTurnStatus, TURN_REQUEST_KIND,
        TURN_RESULT_KIND,
    },
    media::MediaStore,
    tools::{
        MEDIA_PUBLISH_REQUEST_KIND, MEDIA_PUBLISH_RESULT_KIND, MediaPublishRequest,
        MediaPublishResult, ROOM_MEMBERS_TOOL_REQUEST_KIND, ROOM_MEMBERS_TOOL_RESULT_KIND,
        RoomMembersResult, RoomMembersToolRequest, RoomToolsResident,
    },
};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::{sleep, timeout},
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

async fn start_fake(
    harness: &NormaHarness,
    name: &str,
    log: Log,
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
            if name == "alpha" {
                sleep(Duration::from_millis(25)).await;
            }
            let result = LlmTurnResult {
                request_id: request.request_id.clone(),
                thread_id: Some(format!("{name}-thread")),
                turn_id: Some(request.request_id),
                status: LlmTurnStatus::Completed,
                final_response: Some(format!("{name}-answer")),
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
    timeout(Duration::from_secs(3), async {
        loop {
            let room = chat.room(id).await.unwrap();
            if !room.busy {
                return room;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("room should finish")
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
    let (alpha_id, alpha_worker) = start_fake(&harness, "alpha", log.clone()).await?;
    let (beta_id, beta_worker) = start_fake(&harness, "beta", log.clone()).await?;

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
    let done = wait_done(&chat, group.id).await;
    assert_eq!(
        done.messages
            .iter()
            .map(|message| message.role.as_str())
            .collect::<Vec<_>>(),
        vec!["user", "agent", "agent", "summary"]
    );
    let calls = log.lock().await.clone();
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[1].0, "alpha");
    assert_eq!(calls[2].0, "beta");
    assert!(calls[2].1.prompt.contains("alpha-answer"));
    assert!(calls[3].1.prompt.contains("beta-answer"));
    assert!(calls.iter().all(|(_, request)| request.thread_id.is_none()));
    assert!(calls[1].1.context.iter().any(|event| {
        event.room_id == solo.id && event.role == "user" && event.text == "Handle solo"
    }));
    assert!(calls[2].1.context.iter().any(|event| {
        event.room_id == group.id && event.author == "alpha" && event.text == "alpha-answer"
    }));
    assert!(
        calls[2]
            .1
            .context
            .iter()
            .all(|event| event.room_id != solo.id)
    );
    assert!(calls[3].1.context.iter().any(|event| {
        event.room_id == group.id && event.author == "beta" && event.text == "beta-answer"
    }));

    chat.send_user_message(group.id, "Follow up".into()).await?;
    wait_done(&chat, group.id).await;
    let calls = log.lock().await.clone();
    assert!(calls[4].1.context.iter().any(|event| {
        event.room_id == solo.id && event.role == "user" && event.text == "Handle solo"
    }));
    assert!(
        calls[5]
            .1
            .context
            .iter()
            .all(|event| event.room_id == group.id)
    );

    chat.send_user_message(group.id, "@beta Please answer this".into())
        .await?;
    let done = wait_done(&chat, group.id).await;
    assert_eq!(done.messages.last().unwrap().author, "beta");
    let calls = log.lock().await.clone();
    assert_eq!(calls.len(), 8, "a mention calls only the named Resident");
    assert_eq!(calls[7].0, "beta");
    let origin = calls[7].1.origin.as_ref().expect("group origin");
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
    assert_eq!(calls.len(), 9);
    assert_eq!(calls[8].0, "alpha");
    assert!(calls[8].1.context.iter().any(|event| {
        event.room_id == group.id
            && event.role == "user"
            && event.text == "@beta Please answer this"
    }));
    assert!(calls[8].1.context.iter().any(|event| {
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
    let result = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(ResidentEvent::Message(message)) = alpha_rx.recv().await {
                if message.message().kind().as_str() == ROOM_MEMBERS_TOOL_RESULT_KIND {
                    break serde_json::from_value::<RoomMembersResult>(
                        message.message().payload().clone(),
                    );
                }
            }
        }
    })
    .await??;
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
    let result: MediaPublishResult = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(ResidentEvent::Message(message)) = receiver.recv().await {
                if message.message().kind().as_str() == MEDIA_PUBLISH_RESULT_KIND {
                    break serde_json::from_value(message.message().payload().clone());
                }
            }
        }
    })
    .await??;
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
    assert_eq!(alpha_calls.len(), 3);
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
    chat_runtime
        .resident()
        .send_user_message(
            group.id,
            "@codex 请调用 list_group_members 工具查询当前群成员，然后仅输出工具返回的成员名称。"
                .into(),
        )
        .await?;
    let room = timeout(Duration::from_secs(330), async {
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
    .await?;
    let answer = room.messages.last().expect("Codex answer");
    assert_eq!(answer.role, "agent", "{answer:?}");
    assert!(
        answer.text.contains("codex") && answer.text.contains("beta"),
        "{answer:?}"
    );
    codex_runtime.shutdown().await?;
    tools_runtime.shutdown().await?;
    chat_runtime.shutdown().await?;
    harness.registration_sender().unregister(beta_id).await?;
    Ok(())
}
