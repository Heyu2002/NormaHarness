use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CapabilityKey, FlowPacket, MachineKey, MessageKind, ResidentEffect, ResidentKey, SnapshotId,
    StateId, StateMachineDefinition, ThreadId,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResidentProfile {
    pub key: ResidentKey,
    pub accepted_messages: BTreeSet<MessageKind>,
    #[serde(default)]
    pub provided_capabilities: BTreeSet<CapabilityKey>,
    pub state_machines: Vec<StateMachineDefinition>,
}

impl ResidentProfile {
    /// Starts a profile for one Resident with no declared messages,
    /// capabilities, or state machines.
    pub fn new(key: ResidentKey) -> Self {
        Self {
            key,
            accepted_messages: BTreeSet::new(),
            provided_capabilities: BTreeSet::new(),
            state_machines: Vec::new(),
        }
    }

    /// Declares one message kind accepted by this Resident.
    pub fn with_accepted_message(mut self, kind: MessageKind) -> Self {
        self.accepted_messages.insert(kind);
        self
    }

    /// Declares one capability provided by this Resident.
    pub fn with_provided_capability(mut self, capability: CapabilityKey) -> Self {
        self.provided_capabilities.insert(capability);
        self
    }

    /// Adds one state-machine definition owned by this Resident.
    pub fn with_state_machine(mut self, definition: StateMachineDefinition) -> Self {
        self.state_machines.push(definition);
        self
    }

    pub fn accepts(&self, kind: &MessageKind) -> bool {
        self.accepted_messages.is_empty() || self.accepted_messages.contains(kind)
    }
}

/// Public, data-only information about one registered Resident.
///
/// This is an address-book entry, not a route or an instance handle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResidentDescriptor {
    pub key: ResidentKey,
    pub accepted_messages: BTreeSet<MessageKind>,
    pub provided_capabilities: BTreeSet<CapabilityKey>,
}

impl ResidentDescriptor {
    pub fn accepts(&self, kind: &MessageKind) -> bool {
        self.accepted_messages.is_empty() || self.accepted_messages.contains(kind)
    }

    pub fn provides(&self, capability: &CapabilityKey) -> bool {
        self.provided_capabilities.contains(capability)
    }
}

/// A safe projection of one immutable RDF registration snapshot.
///
/// It intentionally has no factory, compiled module, mailbox, Store, Gate,
/// exposure ID, or callback. Residents may retain this value as ordinary data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResidentDirectorySnapshot {
    pub snapshot_id: SnapshotId,
    pub residents: BTreeMap<ResidentKey, ResidentDescriptor>,
}

impl ResidentDirectorySnapshot {
    pub fn get(&self, key: &ResidentKey) -> Option<&ResidentDescriptor> {
        self.residents.get(key)
    }

    pub fn providers(
        &self,
        capability: &CapabilityKey,
    ) -> impl Iterator<Item = &ResidentDescriptor> {
        self.residents
            .values()
            .filter(|resident| resident.provides(capability))
    }
}

/// A data-only view of the current AgentThread context.
///
/// A Resident receives this value by copy for one delivery. It deliberately
/// contains no host capability, callback, object reference, or mutable handle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResidentContextSnapshot {
    pub thread_id: ThreadId,
    pub active_snapshot: SnapshotId,
    pub machine_states: BTreeMap<MachineKey, StateId>,
    pub directory: ResidentDirectorySnapshot,
}

/// The only input delivered to an isolated Resident.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResidentInvocation {
    pub packet: FlowPacket,
    pub context: ResidentContextSnapshot,
}

/// The only output accepted from an isolated Resident.
///
/// Transport metadata such as correlation and hop count is not present here.
/// RTDF owns that metadata and constructs packets after decoding this value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum ResidentResponse {
    Ok(ResidentEffect),
    Error(ResidentError),
}

#[derive(Clone, Debug, Error, Eq, PartialEq, Serialize, Deserialize)]
#[error("resident execution failed [{code}]: {message}")]
pub struct ResidentError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl ResidentError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }
}
