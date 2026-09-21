use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{MessageKind, ResidentKey, SnapshotId, StateEvent};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlowMessage {
    pub kind: MessageKind,
    pub payload: Value,
}

impl FlowMessage {
    pub fn new(kind: MessageKind, payload: Value) -> Self {
        Self { kind, payload }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum FlowTarget {
    Resident(ResidentKey),
    Return,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlowPacket {
    pub message_id: Uuid,
    pub correlation_id: Uuid,
    pub target: FlowTarget,
    pub hop: u32,
    pub message: FlowMessage,
}

impl FlowPacket {
    pub fn ingress(target: ResidentKey, message: FlowMessage) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            target: FlowTarget::Resident(target),
            hop: 0,
            message,
        }
    }

    pub fn emitted(&self, emission: Emission) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            correlation_id: self.correlation_id,
            target: emission.target,
            hop: self.hop.saturating_add(1),
            message: emission.message,
        }
    }

    pub fn redirected_to(&self, target: FlowTarget) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            correlation_id: self.correlation_id,
            target,
            hop: self.hop.saturating_add(1),
            message: self.message.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Emission {
    pub target: FlowTarget,
    pub message: FlowMessage,
}

impl Emission {
    pub fn to(target: ResidentKey, message: FlowMessage) -> Self {
        Self {
            target: FlowTarget::Resident(target),
            message,
        }
    }

    pub fn return_to_caller(message: FlowMessage) -> Self {
        Self {
            target: FlowTarget::Return,
            message,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResidentEffect {
    pub emissions: Vec<Emission>,
    pub state_events: Vec<StateEvent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TurnLimits {
    pub max_hops_per_packet: u32,
    pub max_processed_packets: usize,
}

impl Default for TurnLimits {
    fn default() -> Self {
        Self {
            max_hops_per_packet: 256,
            max_processed_packets: 10_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnOutput {
    pub snapshot_id: SnapshotId,
    pub processed_packets: usize,
    pub returned: Vec<FlowMessage>,
}
