use std::sync::Arc;

use village_harness_protocol::{BatchId, GateFactory, GateKey, ResidentKey};

use crate::{NativeResidentRegistration, ResidentArtifact};

pub struct RegistrationBatch {
    pub(crate) id: BatchId,
    pub(crate) resident_ops: Vec<ResidentRegistration>,
    pub(crate) gate_ops: Vec<GateRegistration>,
}

impl RegistrationBatch {
    pub fn new() -> Self {
        Self {
            id: BatchId::new(),
            resident_ops: Vec::new(),
            gate_ops: Vec::new(),
        }
    }

    pub fn id(&self) -> BatchId {
        self.id
    }

    pub fn upsert_resident(mut self, artifact: ResidentArtifact) -> Self {
        self.resident_ops
            .push(ResidentRegistration::UpsertWasm(artifact));
        self
    }

    pub fn upsert_native_resident(mut self, registration: NativeResidentRegistration) -> Self {
        self.resident_ops
            .push(ResidentRegistration::UpsertNative(registration));
        self
    }

    pub fn remove_resident(mut self, key: ResidentKey) -> Self {
        self.resident_ops.push(ResidentRegistration::Remove(key));
        self
    }

    pub fn upsert_gate(mut self, factory: Arc<dyn GateFactory>) -> Self {
        self.gate_ops.push(GateRegistration::Upsert(factory));
        self
    }

    pub fn remove_gate(mut self, key: GateKey) -> Self {
        self.gate_ops.push(GateRegistration::Remove(key));
        self
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.resident_ops.is_empty() && self.gate_ops.is_empty()
    }
}

impl Default for RegistrationBatch {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) enum ResidentRegistration {
    UpsertWasm(ResidentArtifact),
    UpsertNative(NativeResidentRegistration),
    Remove(ResidentKey),
}

pub(crate) enum GateRegistration {
    Upsert(Arc<dyn GateFactory>),
    Remove(GateKey),
}

impl std::fmt::Debug for RegistrationBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegistrationBatch")
            .field("id", &self.id)
            .field("resident_operation_count", &self.resident_ops.len())
            .field("gate_operation_count", &self.gate_ops.len())
            .finish()
    }
}
