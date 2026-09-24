//! Common turn protocol for Residents advertising the `llm` capability.
//! Other implementations can join chat rooms by handling these message kinds.

use crate::media::MediaAsset;
use async_trait::async_trait;
use norma_harness::{FlowMessage, Gate, GateContext, GateError, MessageKind};
use serde::{Deserialize, Serialize};

/// Advertise this capability only when the Resident handles the turn protocol below.
pub const LLM_CAPABILITY: &str = "llm";
pub const TURN_REQUEST_KIND: &str = "llm.turn.request";
pub const TURN_RESULT_KIND: &str = "llm.turn.result";
pub const MEMORY_TRIGGER_KIND: &str = "llm.memory.trigger";

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryTriggerReason {
    Sleep,
    Compaction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryTrigger {
    pub room_id: u64,
    pub reason: MemoryTriggerReason,
    pub sleep_generation: u64,
    pub context: Vec<LlmContextMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmRoomKind {
    Solo,
    Group,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmChatTurnKind {
    Direct,
    Contribution,
    Mention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmOrigin {
    pub kind: LlmRoomKind,
    pub room_id: u64,
    pub room_name: String,
    #[serde(default)]
    pub incognito: bool,
}

/// `request_id` must be echoed in the result. Context events let a Resident
/// observe messages from every room it has joined, including turns where it
/// was not asked to speak. Conversation continuity belongs to the Resident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmContextMessage {
    pub id: u64,
    pub room_id: u64,
    pub room_name: String,
    pub role: String,
    pub author: String,
    pub text: String,
    #[serde(default)]
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<MediaAsset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmTurnRequest {
    pub request_id: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<LlmOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_resident: Option<String>,
    /// Chat-specific notice for adapters that fetch room messages with tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_turn_kind: Option<LlmChatTurnKind>,
    /// Optional provider thread to seed a Resident conversation. Chat leaves
    /// this unset; the Resident maps room IDs to provider threads internally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<LlmContextMessage>,
}

/// Shared inbound boundary for LLM Residents. Chat supplies the room origin;
/// the Gate validates it and stamps the authenticated sending Resident.
#[derive(Debug, Default)]
pub struct LlmContextGate;

#[async_trait]
impl Gate for LlmContextGate {
    async fn pass(
        &self,
        context: &GateContext,
        message: FlowMessage,
    ) -> Result<FlowMessage, GateError> {
        if message.kind().as_str() != TURN_REQUEST_KIND {
            return Ok(message);
        }
        let (kind, payload) = message.into_parts();
        let mut request: LlmTurnRequest = serde_json::from_value(payload)
            .map_err(|error| GateError::new(format!("invalid LLM context JSON: {error}")))?;
        request.validate().map_err(GateError::new)?;
        let origin = request
            .origin
            .as_ref()
            .ok_or_else(|| GateError::new("LLM turn requires a solo or group origin"))?;
        if origin.room_id == 0 || origin.room_name.trim().is_empty() {
            return Err(GateError::new("LLM turn origin needs a room ID and name"));
        }
        request.source_resident = Some(context.source().to_string());
        let payload = serde_json::to_value(request)
            .map_err(|error| GateError::new(format!("could not encode LLM context: {error}")))?;
        Ok(FlowMessage::new(kind, payload))
    }
}

impl LlmTurnRequest {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.request_id.trim().is_empty() {
            return Err("request_id must not be empty");
        }
        if self.prompt.trim().is_empty() {
            return Err("prompt must not be empty");
        }
        if self
            .thread_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty())
        {
            return Err("thread_id must not be empty when present");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LlmTurnStatus {
    Completed,
    Failed,
    Interrupted,
}

/// Send this result to the source Resident through RTDF, even on failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmTurnResult {
    pub request_id: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub status: LlmTurnStatus,
    pub final_response: Option<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub compacted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<MediaAsset>,
}

impl LlmTurnResult {
    pub(crate) fn failed(request_id: String, error: impl Into<String>) -> Self {
        Self {
            request_id,
            thread_id: None,
            turn_id: None,
            status: LlmTurnStatus::Failed,
            final_response: None,
            error: Some(error.into()),
            compacted: false,
            attachments: Vec::new(),
        }
    }
}

#[must_use]
pub fn turn_request_message(request: &LlmTurnRequest) -> FlowMessage {
    FlowMessage::new(
        MessageKind::new(TURN_REQUEST_KIND).expect("static message kind is valid"),
        serde_json::to_value(request).expect("LLM request is serializable"),
    )
}

#[cfg(test)]
mod tests {
    use super::{LlmContextGate, LlmOrigin, LlmRoomKind, LlmTurnRequest, turn_request_message};
    use norma_harness::{Gate, GateContext, ResidentKey};

    #[tokio::test]
    async fn gate_stamps_source_and_requires_named_room() {
        let gate = LlmContextGate;
        let context = GateContext::new(
            ResidentKey::new("chat.rooms").unwrap(),
            ResidentKey::new("codex").unwrap(),
        );
        let mut request = LlmTurnRequest {
            request_id: "turn-1".into(),
            prompt: "你好".into(),
            origin: Some(LlmOrigin {
                kind: LlmRoomKind::Group,
                room_id: 7,
                room_name: "设计群".into(),
                incognito: false,
            }),
            source_resident: Some("spoofed.source".into()),
            chat_turn_kind: None,
            thread_id: None,
            context: Vec::new(),
        };
        let passed = gate
            .pass(&context, turn_request_message(&request))
            .await
            .unwrap();
        let passed: LlmTurnRequest = serde_json::from_value(passed.payload().clone()).unwrap();
        assert_eq!(passed.source_resident.as_deref(), Some("chat.rooms"));
        assert_eq!(passed.origin.unwrap().room_name, "设计群");
        request.origin = None;
        assert!(
            gate.pass(&context, turn_request_message(&request))
                .await
                .is_err()
        );
    }
}
