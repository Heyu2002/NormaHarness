use std::{
    collections::{HashSet, VecDeque},
    path::Path,
    process::Stdio,
};

use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

use super::{CodexResident, CodexSandbox, CodexTurnRequest, CodexTurnResult, CodexTurnStatus};
use crate::{
    media::{MAX_MESSAGE_MEDIA, MediaAsset},
    tools::MediaPublishRequest,
};

#[derive(Debug, Error)]
pub(crate) enum AppServerError {
    #[error("Codex app-server I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Codex app-server sent invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Codex app-server protocol error: {0}")]
    Protocol(String),
}

/// One stdio connection. The Resident serializes turns on this connection.
pub(crate) struct CodexAppServer {
    _child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    pending: VecDeque<Value>,
    loaded_threads: HashSet<String>,
    next_id: u64,
    published_media: Vec<MediaAsset>,
}

impl CodexAppServer {
    pub(crate) async fn start(program: &Path, cwd: &Path) -> Result<Self, AppServerError> {
        let mut child = Command::new(program)
            .arg("app-server")
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AppServerError::Protocol("missing app-server stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppServerError::Protocol("missing app-server stdout".into()))?;
        let mut server = Self {
            _child: child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            pending: VecDeque::new(),
            loaded_threads: HashSet::new(),
            next_id: 0,
            published_media: Vec::new(),
        };
        server
            .call(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "norma_codex_resident",
                        "title": "Norma Codex Resident",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {"experimentalApi": true, "requestAttestation": false}
                }),
            )
            .await?;
        server
            .write(&json!({"method": "initialized", "params": {}}))
            .await?;
        Ok(server)
    }

    pub(crate) async fn run_turn(
        &mut self,
        request: &CodexTurnRequest,
        resident: &CodexResident,
        cwd: &Path,
        model: Option<&str>,
        effort: Option<&str>,
        sandbox: CodexSandbox,
    ) -> Result<CodexTurnResult, AppServerError> {
        self.published_media.clear();
        let cwd = cwd
            .to_str()
            .ok_or_else(|| AppServerError::Protocol("working directory is not UTF-8".into()))?;
        let thread_id = match &request.thread_id {
            Some(id) => {
                if !self.loaded_threads.contains(id) {
                    self.call("thread/resume", json!({"threadId": id})).await?;
                    self.loaded_threads.insert(id.clone());
                }
                id.clone()
            }
            None => {
                let mut params = json!({
                    "cwd": cwd,
                    "approvalPolicy": "never",
                    "serviceName": "norma_codex_resident",
                    "dynamicTools": [{
                        "type": "function",
                        "name": "list_group_members",
                        "description": "查询当前群聊中的 LLM Resident 成员。仅在当前请求来自群聊时使用。无需参数。",
                        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
                    }, {
                        "type": "function",
                        "name": "publish_media",
                        "description": "把生成的 PNG、JPEG、WebP 或 GIF 图片发布为当前聊天的可预览、可下载附件。图片生成后必须调用此工具；只在回复中写文件名不会把文件交给用户。单次回复最多 4 个，每个最多 8 MiB。",
                        "inputSchema": {"type": "object", "properties": {
                            "filename": {"type": "string"},
                            "mime_type": {"type": "string", "enum": ["image/png", "image/jpeg", "image/webp", "image/gif"]},
                            "base64": {"type": "string", "description": "完整图片文件内容的标准 base64，不包含 data URL 前缀"}
                        }, "required": ["filename", "mime_type", "base64"], "additionalProperties": false}
                    }]
                });
                if let Some(model) = model {
                    params["model"] = json!(model);
                }
                let response = self.call("thread/start", params).await?;
                let id = string_at(&response, &["thread", "id"], "thread/start.thread.id")?;
                self.loaded_threads.insert(id.clone());
                id
            }
        };

        let mut inputs = vec![json!({"type": "text", "text": request.prompt})];
        for event in &request.context {
            for attachment in &event.attachments {
                for path in &attachment.model_paths {
                    inputs.push(json!({"type": "localImage", "path": path}));
                }
            }
        }
        let mut params = json!({
            "threadId": thread_id,
            "input": inputs,
            "cwd": cwd,
            "approvalPolicy": "never",
            "sandboxPolicy": sandbox.policy(cwd)
        });
        if let Some(model) = model {
            params["model"] = json!(model);
        }
        if let Some(effort) = effort {
            params["effort"] = json!(effort);
        }
        let response = self
            .call_with_tool("turn/start", params, Some((resident, request)))
            .await?;
        let turn_id = string_at(&response, &["turn", "id"], "turn/start.turn.id")?;
        let mut final_response = None;
        let mut last_agent_message = None;
        let mut compacted = false;
        let mut generated_images: Vec<(String, String, String)> = Vec::new();

        loop {
            let event = match self.pending.pop_front() {
                Some(event) => event,
                None => self.read().await?,
            };
            if event.get("id").is_some() && event.get("method").is_some() {
                if event["method"] == "item/tool/call"
                    && (event["params"]["threadId"] != thread_id
                        || event["params"]["turnId"] != turn_id)
                {
                    self.answer_server_request(&event, None).await?;
                } else {
                    self.answer_server_request(&event, Some((resident, request)))
                        .await?;
                }
                continue;
            }
            match event.get("method").and_then(Value::as_str) {
                Some("item/completed") => {
                    let params = &event["params"];
                    if params["turnId"].as_str().is_some_and(|id| id != turn_id) {
                        continue;
                    }
                    let item = &params["item"];
                    if item["type"] == "contextCompaction" {
                        compacted = true;
                    } else if item["type"] == "agentMessage" {
                        if let Some(text) = item["text"].as_str() {
                            last_agent_message = Some(text.to_owned());
                            if item["phase"] == "final_answer" {
                                final_response = Some(text.to_owned());
                            }
                        }
                    } else if item["type"] == "imageGeneration" {
                        if let Some(result) = item["result"].as_str() {
                            if let Some((mime, encoded)) = generated_image_payload(result) {
                                let extension = mime.split('/').next_back().unwrap_or("png");
                                generated_images.push((
                                    format!(
                                        "generated-{}.{}",
                                        generated_images.len() + 1,
                                        extension
                                    ),
                                    mime.to_owned(),
                                    encoded.to_owned(),
                                ));
                            }
                        }
                    }
                }
                Some("turn/completed") if event["params"]["turn"]["id"] == turn_id => {
                    let turn = &event["params"]["turn"];
                    if self.published_media.is_empty() {
                        for (index, (filename, mime_type, base64)) in generated_images
                            .into_iter()
                            .take(MAX_MESSAGE_MEDIA)
                            .enumerate()
                        {
                            let publication = MediaPublishRequest {
                                call_id: format!("generated-{turn_id}-{index}"),
                                filename,
                                mime_type,
                                base64,
                                incognito: request
                                    .origin
                                    .as_ref()
                                    .is_some_and(|origin| origin.incognito),
                            };
                            if let Ok(asset) = resident.invoke_publish_media_tool(publication).await
                            {
                                self.published_media.push(asset);
                            }
                        }
                    }
                    let status = match turn["status"].as_str() {
                        Some("completed") => CodexTurnStatus::Completed,
                        Some("interrupted") => CodexTurnStatus::Interrupted,
                        Some("failed") => CodexTurnStatus::Failed,
                        other => {
                            return Err(AppServerError::Protocol(format!(
                                "unknown turn status: {other:?}"
                            )));
                        }
                    };
                    let error = turn["error"]["message"].as_str().map(str::to_owned);
                    return Ok(CodexTurnResult {
                        request_id: request.request_id.clone(),
                        thread_id: Some(thread_id),
                        turn_id: Some(turn_id),
                        status,
                        final_response: final_response.or(last_agent_message),
                        error,
                        compacted,
                        attachments: std::mem::take(&mut self.published_media),
                    });
                }
                _ => {}
            }
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, AppServerError> {
        self.call_with_tool(method, params, None).await
    }

    async fn call_with_tool(
        &mut self,
        method: &str,
        params: Value,
        tool_context: Option<(&CodexResident, &CodexTurnRequest)>,
    ) -> Result<Value, AppServerError> {
        self.next_id += 1;
        let id = self.next_id;
        self.write(&json!({"id": id, "method": method, "params": params}))
            .await?;
        loop {
            let message = self.read().await?;
            if message.get("id").and_then(Value::as_u64) == Some(id) {
                if !message["error"].is_null() {
                    return Err(AppServerError::Protocol(format!(
                        "{method}: {}",
                        message["error"]
                    )));
                }
                return message.get("result").cloned().ok_or_else(|| {
                    AppServerError::Protocol(format!("{method} returned no result"))
                });
            }
            if message.get("id").is_some() && message.get("method").is_some() {
                self.answer_server_request(&message, tool_context).await?;
            } else if message.get("method").is_some() {
                self.pending.push_back(message);
            }
        }
    }

    async fn answer_server_request(
        &mut self,
        request: &Value,
        tool_context: Option<(&CodexResident, &CodexTurnRequest)>,
    ) -> Result<(), AppServerError> {
        let id = &request["id"];
        let method = request["method"].as_str().unwrap_or_default();
        if method == "item/tool/call" {
            let params = &request["params"];
            let output = if params["namespace"].is_null() && params["tool"] == "list_group_members"
            {
                match tool_context {
                    Some((resident, turn)) => resident
                        .invoke_room_members_tool(
                            turn.origin.as_ref(),
                            params["callId"].as_str().unwrap_or_default(),
                        )
                        .await
                        .map(|result| {
                            let success = result.error.is_none();
                            (
                                success,
                                serde_json::to_string(&result).expect("serializable members"),
                            )
                        })
                        .unwrap_or_else(|error| (false, error)),
                    None => (false, "tool call is outside the active turn".into()),
                }
            } else if params["namespace"].is_null() && params["tool"] == "publish_media" {
                match tool_context {
                    Some((resident, turn)) if self.published_media.len() < MAX_MESSAGE_MEDIA => {
                        let args = &params["arguments"];
                        let request = MediaPublishRequest {
                            call_id: params["callId"].as_str().unwrap_or_default().to_owned(),
                            filename: args["filename"].as_str().unwrap_or_default().to_owned(),
                            mime_type: args["mime_type"].as_str().unwrap_or_default().to_owned(),
                            base64: args["base64"].as_str().unwrap_or_default().to_owned(),
                            incognito: turn.origin.as_ref().is_some_and(|origin| origin.incognito),
                        };
                        match resident.invoke_publish_media_tool(request).await {
                            Ok(asset) => {
                                let view = asset.view();
                                self.published_media.push(asset);
                                (
                                    true,
                                    serde_json::to_string(&view).expect("serializable media view"),
                                )
                            }
                            Err(error) => (false, error),
                        }
                    }
                    Some(_) => (false, "at most 4 images per reply".into()),
                    None => (false, "tool call is outside the active turn".into()),
                }
            } else {
                (false, "unknown dynamic tool".into())
            };
            self.write(&json!({"id": id, "result": {
                "contentItems": [{"type": "inputText", "text": output.1}],
                "success": output.0
            }}))
            .await
        } else if matches!(
            method,
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
        ) {
            self.write(&json!({"id": id, "result": {"decision": "decline"}}))
                .await
        } else {
            self.write(&json!({
                "id": id,
                "error": {"code": -32601, "message": format!("unsupported client request: {method}")}
            }))
            .await
        }
    }

    async fn write(&mut self, message: &Value) -> Result<(), AppServerError> {
        self.stdin.write_all(message.to_string().as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Value, AppServerError> {
        let line = self
            .stdout
            .next_line()
            .await?
            .ok_or_else(|| AppServerError::Protocol("app-server closed stdout".into()))?;
        Ok(serde_json::from_str(&line)?)
    }
}

fn string_at(value: &Value, path: &[&str], label: &str) -> Result<String, AppServerError> {
    let mut current = value;
    for key in path {
        current = &current[*key];
    }
    current
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| AppServerError::Protocol(format!("missing {label}")))
}

fn generated_image_payload(result: &str) -> Option<(&'static str, &str)> {
    let payload = result
        .split_once(";base64,")
        .map_or(result, |(_, data)| data);
    let mime = if payload.starts_with("iVBOR") {
        "image/png"
    } else if payload.starts_with("/9j/") {
        "image/jpeg"
    } else if payload.starts_with("R0lGOD") {
        "image/gif"
    } else if payload.starts_with("UklGR") {
        "image/webp"
    } else {
        return None;
    };
    Some((mime, payload))
}
