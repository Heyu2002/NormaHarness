use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{StatusCode, header},
    response::{
        Html, IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use norma_harness::{CapabilityKey, NormaHarness, ResidentKey};
use norma_residents::{
    chat::{ChatError, ChatResident, ChatResidentRuntime, ChatRoom, storage::ChatStorage},
    codex::{CodexResident, CodexResidentConfig, CodexSandbox},
    media::{MAX_MEDIA_BYTES, MAX_MESSAGE_MEDIA, MediaStore},
    memory::MemoryManager,
    tools::RoomToolsResident,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

#[derive(Clone)]
struct AppState {
    harness: Arc<NormaHarness>,
    chat: Arc<ChatResident>,
    model_names: Arc<BTreeMap<String, String>>,
    media: Arc<MediaStore>,
    private_media: Arc<MediaStore>,
    memory: Arc<MemoryManager>,
}

#[derive(Serialize)]
struct OnlineModel {
    key: String,
    model: String,
    provider: &'static str,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(ErrorBody { error: self.1 })).into_response()
    }
}

impl From<ChatError> for ApiError {
    fn from(error: ChatError) -> Self {
        let status = match error {
            ChatError::Invalid(_) => StatusCode::BAD_REQUEST,
            ChatError::NotFound => StatusCode::NOT_FOUND,
            ChatError::Busy => StatusCode::CONFLICT,
            ChatError::Internal(_) => StatusCode::BAD_GATEWAY,
        };
        Self(status, error.to_string())
    }
}

#[derive(Deserialize)]
struct CreateRoom {
    name: String,
    members: Vec<String>,
    #[serde(default)]
    incognito: bool,
}

#[derive(Deserialize)]
struct SendMessage {
    text: String,
}

#[derive(Deserialize)]
struct ClientPresence {
    client: String,
}

#[derive(Deserialize)]
struct ArchiveRoom {
    archived: bool,
}

#[derive(Deserialize)]
struct SetMemory {
    enabled: bool,
}

async fn memory_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.memory.status())
}

async fn set_memory(
    State(state): State<AppState>,
    Json(input): Json<SetMemory>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .memory
        .set_enabled(input.enabled)
        .map(Json)
        .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RestoreSnapshot {
    Rooms(Vec<ChatRoom>),
    WithMedia {
        rooms: Vec<ChatRoom>,
        media: Vec<RestoreMedia>,
    },
}

#[derive(Deserialize)]
struct RestoreMedia {
    id: String,
    name: String,
    mime_type: String,
    base64: String,
}

async fn residents(State(state): State<AppState>) -> impl IntoResponse {
    let llm = CapabilityKey::new("llm").expect("static capability is valid");
    let codex = CapabilityKey::new("codex.agent").expect("static capability is valid");
    let models = state
        .harness
        .rdf()
        .providers(&llm)
        .await
        .into_iter()
        .map(|resident| {
            let key = resident.key().as_str();
            OnlineModel {
                key: key.to_owned(),
                model: state
                    .model_names
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| key.to_owned()),
                provider: if resident.provides(&codex) {
                    "Codex"
                } else {
                    "LLM Resident"
                },
            }
        })
        .collect::<Vec<_>>();
    Json(models)
}

async fn rooms(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.chat.list_rooms().await)
}

async fn create_room(
    State(state): State<AppState>,
    Json(input): Json<CreateRoom>,
) -> Result<impl IntoResponse, ApiError> {
    if input.members.len() < 2 || input.members.len() > 8 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "select 2–8 different LLM Residents for a group".into(),
        ));
    }
    let llm = CapabilityKey::new("llm").expect("static capability is valid");
    let mut members = Vec::with_capacity(input.members.len());
    for raw in input.members {
        let key = ResidentKey::new(raw)
            .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
        let descriptor = state.harness.rdf().resident(&key).await;
        if !descriptor.is_some_and(|resident| resident.provides(&llm)) {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("{key} is not an available LLM Resident"),
            ));
        }
        members.push(key);
    }
    let room = state
        .chat
        .create_room_with_mode(input.name, members, input.incognito)
        .await?;
    Ok((StatusCode::CREATED, Json(room)))
}

async fn solo_room(
    State(state): State<AppState>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let key = ResidentKey::new(raw)
        .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
    let llm = CapabilityKey::new("llm").expect("static capability is valid");
    if !state
        .harness
        .rdf()
        .resident(&key)
        .await
        .is_some_and(|resident| resident.provides(&llm))
    {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("{key} is not an online LLM Resident"),
        ));
    }
    let name = state
        .model_names
        .get(key.as_str())
        .cloned()
        .unwrap_or_else(|| key.to_string());
    Ok(Json(state.chat.ensure_solo_room(key, name).await?))
}

async fn incognito_solo_room(
    State(state): State<AppState>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let key = ResidentKey::new(raw)
        .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
    let llm = CapabilityKey::new("llm").expect("static capability is valid");
    if !state
        .harness
        .rdf()
        .resident(&key)
        .await
        .is_some_and(|resident| resident.provides(&llm))
    {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("{key} is not an online LLM Resident"),
        ));
    }
    let name = state
        .model_names
        .get(key.as_str())
        .cloned()
        .unwrap_or_else(|| key.to_string());
    Ok(Json(
        state.chat.ensure_incognito_solo_room(key, name).await?,
    ))
}

async fn room(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .chat
        .room(id)
        .await
        .map(Json)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "room not found".into()))
}

async fn send_message(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Json(input): Json<SendMessage>,
) -> Result<impl IntoResponse, ApiError> {
    let room = state.chat.send_user_message(id, input.text).await?;
    Ok((StatusCode::ACCEPTED, Json(room)))
}

async fn send_upload(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, ApiError> {
    let private = state
        .chat
        .room(id)
        .await
        .ok_or(ChatError::NotFound)?
        .incognito;
    let media_store = if private {
        &state.private_media
    } else {
        &state.media
    };
    let mut text = String::new();
    let mut assets = Vec::new();
    let outcome: Result<_, ApiError> =
        async {
            while let Some(field) = multipart
                .next_field()
                .await
                .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?
            {
                match field.name() {
                    Some("text") => {
                        text = field.text().await.map_err(|error| {
                            ApiError(StatusCode::BAD_REQUEST, error.to_string())
                        })?;
                    }
                    Some("files") => {
                        if assets.len() >= MAX_MESSAGE_MEDIA {
                            return Err(ApiError(
                                StatusCode::BAD_REQUEST,
                                format!("最多上传 {MAX_MESSAGE_MEDIA} 张图片"),
                            ));
                        }
                        let filename = field.file_name().unwrap_or("image").to_owned();
                        let bytes = field.bytes().await.map_err(|error| {
                            ApiError(StatusCode::BAD_REQUEST, error.to_string())
                        })?;
                        if bytes.len() > MAX_MEDIA_BYTES {
                            return Err(ApiError(
                                StatusCode::PAYLOAD_TOO_LARGE,
                                "单个图片最多 8 MiB".into(),
                            ));
                        }
                        let asset = media_store
                            .save(&filename, &bytes)
                            .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error))?;
                        assets.push(asset);
                    }
                    _ => return Err(ApiError(StatusCode::BAD_REQUEST, "未知的上传字段".into())),
                }
            }
            state
                .chat
                .send_user_message_with_media(id, text, assets.clone())
                .await
                .map_err(ApiError::from)
        }
        .await;
    match outcome {
        Ok(room) => Ok((StatusCode::ACCEPTED, Json(room))),
        Err(error) => {
            for asset in assets {
                media_store.remove(&asset.id);
            }
            Err(error)
        }
    }
}

async fn media(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let asset = state
        .media
        .get(&id)
        .or_else(|| state.private_media.get(&id))
        .ok_or(ApiError(StatusCode::NOT_FOUND, "image not found".into()))?;
    let bytes = std::fs::read(&asset.path)
        .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok((
        [
            (header::CONTENT_TYPE, asset.mime_type),
            (header::CACHE_CONTROL, "private, max-age=3600".into()),
        ],
        bytes,
    ))
}

async fn events(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let mut updates = state.chat.subscribe(id).await?;
    let stream = async_stream::stream! {
        yield Ok::<Event, Infallible>(Event::default().event("update").data("{}"));
        while let Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) = updates.recv().await {
            yield Ok::<Event, Infallible>(Event::default().event("update").data("{}"));
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}
async fn css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../static/style.css"),
    )
}
async fn js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        include_str!("../static/app.js"),
    )
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/style.css", get(css))
        .route("/app.js", get(js))
        .route("/api/residents", get(residents))
        .route("/api/memory", get(memory_status).post(set_memory))
        .route("/api/rooms", get(rooms).post(create_room))
        .route("/api/rooms/solo/{key}", post(solo_room))
        .route("/api/rooms/incognito/{key}", post(incognito_solo_room))
        .route("/api/rooms/{id}", get(room))
        .route("/api/rooms/{id}/heartbeat", post(heartbeat))
        .route("/api/rooms/{id}/leave", post(leave))
        .route("/api/rooms/{id}/archive", post(archive_room))
        .route("/api/rooms/{id}/messages", post(send_message))
        .route(
            "/api/rooms/{id}/uploads",
            post(send_upload).layer(DefaultBodyLimit::max(36 * 1024 * 1024)),
        )
        .route("/api/rooms/{id}/events", get(events))
        .route("/api/media/{id}", get(media))
        .with_state(state)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Arc::new(NormaHarness::new());
    let data_root = chat_data_root()?;
    let storage = ChatStorage::new(data_root.join("chat.json"))?;
    let media = Arc::new(MediaStore::open(data_root.join("media"))?);
    let private_media = Arc::new(MediaStore::create()?);
    let memory = Arc::new(MemoryManager::open(data_root.join("memory/codex.json"))?);
    let chat: ChatResidentRuntime =
        ChatResident::launch(harness.registration_sender(), harness.message_sender()).await?;
    if let Some(snapshot) = storage.load()? {
        let assets = snapshot.media;
        let restored = assets
            .iter()
            .map(|asset| (asset.id.clone(), asset.clone()))
            .collect::<HashMap<_, _>>();
        media.restore(assets)?;
        chat.resident()
            .restore_rooms_with_media(snapshot.rooms, &restored)
            .await;
    } else if let Some(snapshot) = std::env::var_os("NORMA_CHAT_RESTORE") {
        let snapshot: RestoreSnapshot = serde_json::from_slice(&std::fs::read(snapshot)?)?;
        match snapshot {
            RestoreSnapshot::Rooms(rooms) => chat.resident().restore_rooms(rooms).await,
            RestoreSnapshot::WithMedia {
                rooms,
                media: files,
            } => {
                let mut restored = HashMap::new();
                for file in files {
                    let asset = media.save_base64(&file.name, &file.mime_type, &file.base64)?;
                    restored.insert(file.id, asset);
                }
                chat.resident()
                    .restore_rooms_with_media(rooms, &restored)
                    .await;
            }
        }
    }
    chat.resident().enable_storage(storage).await?;
    let room_tools = RoomToolsResident::launch_with_media_stores(
        harness.registration_sender(),
        harness.message_sender(),
        media.clone(),
        private_media.clone(),
    )
    .await?;
    let cwd = std::env::var_os("NORMA_CODEX_CWD")
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    let mut config = CodexResidentConfig::new(ResidentKey::new("codex")?, &cwd);
    config.memory = Some(memory.clone());
    config.conversation_path = Some(data_root.join("codex-conversations.json"));
    if let Some(program) = std::env::var_os("NORMA_CODEX_BIN") {
        config.codex_program = program.into();
    }
    if let Ok(effort) = std::env::var("NORMA_CODEX_EFFORT") {
        config.effort = Some(effort);
    }
    if std::env::var("NORMA_CODEX_WORKSPACE_WRITE").as_deref() == Ok("1") {
        config.sandbox = CodexSandbox::WorkspaceWrite;
    }
    let mut model_names = BTreeMap::new();
    model_names.insert(
        config.key.to_string(),
        config
            .model
            .clone()
            .unwrap_or_else(|| "Codex default".into()),
    );
    let codex = CodexResident::launch(
        config,
        harness.registration_sender(),
        harness.message_sender(),
    )
    .await?;
    chat.resident().retry_sleep_memory().await;
    let bind: SocketAddr = std::env::var("NORMA_WEB_BIND")
        .unwrap_or_else(|_| "0.0.0.0:3000".into())
        .parse()?;
    let listener = TcpListener::bind(bind).await?;
    println!("Norma chat: http://{}", listener.local_addr()?);
    let state = AppState {
        harness,
        chat: chat.resident(),
        model_names: Arc::new(model_names),
        media,
        private_media,
        memory,
    };
    let sleep_chat = state.chat.clone();
    let idle_ms = std::env::var("NORMA_THREAD_IDLE_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1800)
        .saturating_mul(1000);
    let sleep_monitor = tokio::spawn(async move {
        let mut ticks = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            ticks.tick().await;
            for room in sleep_chat.sleep_due(idle_ms, 45_000, 20_000).await {
                sleep_chat.after_sleep(room).await;
            }
        }
    });
    axum::serve(listener, app(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    sleep_monitor.abort();
    codex.shutdown().await?;
    room_tools.shutdown().await?;
    chat.shutdown().await?;
    Ok(())
}

async fn heartbeat(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Json(input): Json<ClientPresence>,
) -> Result<StatusCode, ApiError> {
    state.chat.heartbeat(id, &input.client).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn leave(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Json(input): Json<ClientPresence>,
) -> StatusCode {
    state.chat.leave(id, &input.client).await;
    StatusCode::NO_CONTENT
}

async fn archive_room(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Json(input): Json<ArchiveRoom>,
) -> Result<Json<ChatRoom>, ApiError> {
    Ok(Json(state.chat.set_archived(id, input.archived).await?))
}

fn chat_data_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = std::env::var_os("NORMA_DATA_DIR") {
        return Ok(path.into());
    }
    if let Some(path) = std::env::var_os("LOCALAPPDATA") {
        return Ok(PathBuf::from(path).join("NormaHarness"));
    }
    if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("norma-harness"));
    }
    if let Some(path) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(path).join(".local/share/norma-harness"));
    }
    Err("no local data directory is available; set NORMA_DATA_DIR".into())
}
