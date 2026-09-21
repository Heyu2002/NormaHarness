use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::{FlowMessage, FlowTarget, MessageKind, ToolKey};

/// Stable message kind used to request the current catalog from a Tool Resident.
pub const TOOL_LIST_REQUEST_KIND: &str = "tools.list";
/// Stable message kind used by a Tool Resident to return its current catalog.
pub const TOOL_CATALOG_KIND: &str = "tools.catalog";
/// Stable message kind used to request one tool invocation.
pub const TOOL_INVOKE_REQUEST_KIND: &str = "tools.invoke";
/// Stable message kind used to request cancellation of an in-flight invocation.
pub const TOOL_CANCEL_REQUEST_KIND: &str = "tools.cancel";
/// Stable message kind used by a Tool Resident to return an invocation outcome.
pub const TOOL_RESULT_KIND: &str = "tools.result";

/// A data-only description of one tool managed by a Tool Resident.
///
/// The descriptor deliberately contains no callback, implementation handle, or
/// Resident instance. Schemas are JSON Schema values interpreted by the Tool
/// Resident and its callers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub key: ToolKey,
    pub description: String,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

impl ToolDescriptor {
    pub fn new(key: ToolKey, description: impl Into<String>, input_schema: Value) -> Self {
        Self {
            key,
            description: description.into(),
            input_schema,
            output_schema: None,
        }
    }

    pub fn with_output_schema(mut self, output_schema: Value) -> Self {
        self.output_schema = Some(output_schema);
        self
    }
}

/// The current data-only directory exposed by one Tool Resident.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolCatalog {
    pub tools: Vec<ToolDescriptor>,
}

impl ToolCatalog {
    pub fn new(tools: Vec<ToolDescriptor>) -> Self {
        Self { tools }
    }

    pub fn into_message(self) -> FlowMessage {
        protocol_message(TOOL_CATALOG_KIND, self)
    }
}

/// Requests the current tool catalog.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ToolListRequest {
    pub reply_to: FlowTarget,
}

impl ToolListRequest {
    pub fn new(reply_to: FlowTarget) -> Self {
        Self { reply_to }
    }

    pub fn into_message(self) -> FlowMessage {
        protocol_message(TOOL_LIST_REQUEST_KIND, self)
    }
}

/// Requests one invocation from a Tool Resident.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolInvokeRequest {
    pub call_id: Uuid,
    pub tool: ToolKey,
    pub arguments: Value,
    pub reply_to: FlowTarget,
}

impl ToolInvokeRequest {
    pub fn new(tool: ToolKey, arguments: Value, reply_to: FlowTarget) -> Self {
        Self {
            call_id: Uuid::new_v4(),
            tool,
            arguments,
            reply_to,
        }
    }

    pub fn with_call_id(
        call_id: Uuid,
        tool: ToolKey,
        arguments: Value,
        reply_to: FlowTarget,
    ) -> Self {
        Self {
            call_id,
            tool,
            arguments,
            reply_to,
        }
    }

    pub fn into_message(self) -> FlowMessage {
        protocol_message(TOOL_INVOKE_REQUEST_KIND, self)
    }
}

/// Requests cancellation of a previously issued tool invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ToolCancelRequest {
    pub call_id: Uuid,
    pub tool: ToolKey,
    pub reply_to: FlowTarget,
}

impl ToolCancelRequest {
    pub fn new(call_id: Uuid, tool: ToolKey, reply_to: FlowTarget) -> Self {
        Self {
            call_id,
            tool,
            reply_to,
        }
    }

    pub fn into_message(self) -> FlowMessage {
        protocol_message(TOOL_CANCEL_REQUEST_KIND, self)
    }
}

/// The terminal outcome of a tool invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum ToolOutcome {
    Success(Value),
    Error(ToolError),
    Cancelled,
}

/// A tool result correlated to the `call_id` supplied by the caller.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub call_id: Uuid,
    pub tool: ToolKey,
    pub outcome: ToolOutcome,
}

impl ToolResult {
    pub fn success(call_id: Uuid, tool: ToolKey, value: Value) -> Self {
        Self {
            call_id,
            tool,
            outcome: ToolOutcome::Success(value),
        }
    }

    pub fn error(call_id: Uuid, tool: ToolKey, error: ToolError) -> Self {
        Self {
            call_id,
            tool,
            outcome: ToolOutcome::Error(error),
        }
    }

    pub fn cancelled(call_id: Uuid, tool: ToolKey) -> Self {
        Self {
            call_id,
            tool,
            outcome: ToolOutcome::Cancelled,
        }
    }

    pub fn into_message(self) -> FlowMessage {
        protocol_message(TOOL_RESULT_KIND, self)
    }
}

/// A serializable business error returned by an individual tool.
#[derive(Clone, Debug, Error, PartialEq, Serialize, Deserialize)]
#[error("tool execution failed [{code}]: {message}")]
pub struct ToolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ToolError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

fn protocol_message(kind: &'static str, payload: impl Serialize) -> FlowMessage {
    let kind = MessageKind::new(kind).expect("built-in Tool message kind is valid");
    let payload =
        serde_json::to_value(payload).expect("built-in Tool protocol payload is JSON-compatible");
    FlowMessage::new(kind, payload)
}
