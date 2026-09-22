use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{MessageKind, ResidentKey};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowMessage {
    kind: MessageKind,
    payload: Value,
}

impl FlowMessage {
    #[must_use]
    pub fn new(kind: MessageKind, payload: Value) -> Self {
        Self { kind, payload }
    }

    #[must_use]
    pub fn kind(&self) -> &MessageKind {
        &self.kind
    }

    #[must_use]
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    #[must_use]
    pub fn into_parts(self) -> (MessageKind, Value) {
        (self.kind, self.payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutedMessage {
    source: ResidentKey,
    target: ResidentKey,
    message: FlowMessage,
}

impl RoutedMessage {
    #[must_use]
    pub fn new(source: ResidentKey, target: ResidentKey, message: FlowMessage) -> Self {
        Self {
            source,
            target,
            message,
        }
    }

    #[must_use]
    pub fn source(&self) -> &ResidentKey {
        &self.source
    }

    #[must_use]
    pub fn target(&self) -> &ResidentKey {
        &self.target
    }

    #[must_use]
    pub fn message(&self) -> &FlowMessage {
        &self.message
    }

    #[must_use]
    pub fn into_message(self) -> FlowMessage {
        self.message
    }
}
