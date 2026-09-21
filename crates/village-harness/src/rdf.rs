use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use tokio::sync::Mutex;
use village_harness_protocol::{
    BatchId, ExposureId, GateFactory, GateKey, GateProfile, MachineKey, ResidentDescriptor,
    ResidentDirectorySnapshot, ResidentKey, ResidentProfile, SnapshotId, StateMachineDefinition,
};

use crate::{
    RegistrationBatch, RegistrationError,
    registration::{GateRegistration, ResidentRegistration},
    resident_host::ResidentDriver,
};

#[derive(Clone)]
pub struct RegistrationSnapshot {
    id: SnapshotId,
    pub(crate) residents: BTreeMap<ResidentKey, RegisteredResident>,
    pub(crate) gates: BTreeMap<GateKey, RegisteredGate>,
}

impl RegistrationSnapshot {
    pub fn id(&self) -> SnapshotId {
        self.id
    }

    pub fn resident_count(&self) -> usize {
        self.residents.len()
    }

    pub fn gate_count(&self) -> usize {
        self.gates.len()
    }

    pub fn resident_profiles(&self) -> impl Iterator<Item = &ResidentProfile> {
        self.residents.values().map(|entry| &entry.profile)
    }

    pub fn gate_profiles(&self) -> impl Iterator<Item = &GateProfile> {
        self.gates.values().map(|entry| &entry.profile)
    }

    /// Returns the data-only address book that may safely cross into a Resident.
    pub fn resident_directory(&self) -> ResidentDirectorySnapshot {
        ResidentDirectorySnapshot {
            snapshot_id: self.id,
            residents: self
                .residents
                .values()
                .map(|entry| {
                    let descriptor = ResidentDescriptor {
                        key: entry.profile.key.clone(),
                        accepted_messages: entry.profile.accepted_messages.clone(),
                        provided_capabilities: entry.profile.provided_capabilities.clone(),
                    };
                    (descriptor.key.clone(), descriptor)
                })
                .collect(),
        }
    }

    pub(crate) fn machine_definitions(&self) -> BTreeMap<MachineKey, StateMachineDefinition> {
        self.residents
            .values()
            .flat_map(|entry| entry.profile.state_machines.iter().cloned())
            .map(|definition| (definition.key.clone(), definition))
            .collect()
    }
}

impl std::fmt::Debug for RegistrationSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegistrationSnapshot")
            .field("id", &self.id)
            .field("resident_count", &self.residents.len())
            .field("gate_count", &self.gates.len())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct RegisteredResident {
    pub exposure_id: ExposureId,
    pub profile: ResidentProfile,
    pub driver: ResidentDriver,
}

#[derive(Clone)]
pub(crate) struct RegisteredGate {
    pub exposure_id: ExposureId,
    pub profile: GateProfile,
    pub factory: Arc<dyn GateFactory>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryFailure {
    pub batch_id: BatchId,
    pub error: RegistrationError,
}

#[derive(Clone, Debug)]
pub struct DiscoveryReport {
    pub snapshot: Arc<RegistrationSnapshot>,
    pub applied_batches: Vec<BatchId>,
    pub failures: Vec<DiscoveryFailure>,
}

#[derive(Debug)]
struct RdfState {
    active: Arc<RegistrationSnapshot>,
    pending: VecDeque<PreparedBatch>,
}

struct PreparedBatch {
    id: BatchId,
    resident_ops: Vec<PreparedResidentRegistration>,
    gate_ops: Vec<PreparedGateRegistration>,
}

enum PreparedResidentRegistration {
    Upsert(Box<RegisteredResident>),
    Remove(ResidentKey),
}

enum PreparedGateRegistration {
    Upsert(RegisteredGate),
    Remove(GateKey),
}

impl std::fmt::Debug for PreparedBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedBatch")
            .field("id", &self.id)
            .field("resident_operation_count", &self.resident_ops.len())
            .field("gate_operation_count", &self.gate_ops.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct RegistrationDataFlow {
    state: Mutex<RdfState>,
}

impl RegistrationDataFlow {
    pub fn new() -> Self {
        let active = RegistrationSnapshot {
            id: SnapshotId::new(),
            residents: BTreeMap::new(),
            gates: BTreeMap::new(),
        };
        Self {
            state: Mutex::new(RdfState {
                active: Arc::new(active),
                pending: VecDeque::new(),
            }),
        }
    }

    pub async fn submit(&self, batch: RegistrationBatch) -> Result<BatchId, RegistrationError> {
        let prepared = prepare_batch(batch)?;
        let id = prepared.id;
        self.state.lock().await.pending.push_back(prepared);
        Ok(id)
    }

    pub async fn discover(&self) -> DiscoveryReport {
        let mut state = self.state.lock().await;
        let mut active = state.active.clone();
        let mut applied_batches = Vec::new();
        let mut failures = Vec::new();

        while let Some(batch) = state.pending.pop_front() {
            match apply_batch(&active, &batch) {
                Ok(candidate) => {
                    active = Arc::new(candidate);
                    applied_batches.push(batch.id);
                }
                Err(error) => failures.push(DiscoveryFailure {
                    batch_id: batch.id,
                    error,
                }),
            }
        }

        state.active = active.clone();
        DiscoveryReport {
            snapshot: active,
            applied_batches,
            failures,
        }
    }

    pub async fn active_snapshot(&self) -> Arc<RegistrationSnapshot> {
        self.state.lock().await.active.clone()
    }
}

impl Default for RegistrationDataFlow {
    fn default() -> Self {
        Self::new()
    }
}

fn prepare_batch(batch: RegistrationBatch) -> Result<PreparedBatch, RegistrationError> {
    if batch.is_empty() {
        return Err(RegistrationError::EmptyBatch);
    }

    let mut resident_keys = BTreeSet::new();
    let mut resident_ops = Vec::with_capacity(batch.resident_ops.len());
    for operation in batch.resident_ops {
        let (key, prepared) = match operation {
            ResidentRegistration::UpsertWasm(artifact) => {
                let profile = artifact.profile().clone();
                let driver = ResidentDriver::compile_wasm(&artifact)?;
                let key = profile.key.clone();
                (
                    key,
                    PreparedResidentRegistration::Upsert(Box::new(RegisteredResident {
                        exposure_id: ExposureId::new(),
                        profile,
                        driver,
                    })),
                )
            }
            ResidentRegistration::UpsertNative(registration) => {
                let profile = registration.profile().clone();
                let driver = ResidentDriver::native(registration)?;
                let key = profile.key.clone();
                (
                    key,
                    PreparedResidentRegistration::Upsert(Box::new(RegisteredResident {
                        exposure_id: ExposureId::new(),
                        profile,
                        driver,
                    })),
                )
            }
            ResidentRegistration::Remove(key) => {
                let identity = key.clone();
                (identity, PreparedResidentRegistration::Remove(key))
            }
        };
        if !resident_keys.insert(key.clone()) {
            return Err(RegistrationError::DuplicateOperation {
                kind: "Resident",
                key: key.to_string(),
            });
        }
        resident_ops.push(prepared);
    }

    let mut gate_keys = BTreeSet::new();
    let mut gate_ops = Vec::with_capacity(batch.gate_ops.len());
    for operation in batch.gate_ops {
        let (key, prepared) = match operation {
            GateRegistration::Upsert(factory) => {
                let profile = factory.profile();
                let key = profile.key.clone();
                (
                    key,
                    PreparedGateRegistration::Upsert(RegisteredGate {
                        exposure_id: ExposureId::new(),
                        profile,
                        factory,
                    }),
                )
            }
            GateRegistration::Remove(key) => {
                let identity = key.clone();
                (identity, PreparedGateRegistration::Remove(key))
            }
        };
        if !gate_keys.insert(key.clone()) {
            return Err(RegistrationError::DuplicateOperation {
                kind: "Gate",
                key: key.to_string(),
            });
        }
        gate_ops.push(prepared);
    }

    Ok(PreparedBatch {
        id: batch.id,
        resident_ops,
        gate_ops,
    })
}

fn apply_batch(
    active: &RegistrationSnapshot,
    batch: &PreparedBatch,
) -> Result<RegistrationSnapshot, RegistrationError> {
    let mut residents = active.residents.clone();
    let mut gates = active.gates.clone();

    for operation in &batch.resident_ops {
        match operation {
            PreparedResidentRegistration::Upsert(entry) => {
                residents.insert(entry.profile.key.clone(), entry.as_ref().clone());
            }
            PreparedResidentRegistration::Remove(key) => {
                residents.remove(key);
            }
        }
    }
    for operation in &batch.gate_ops {
        match operation {
            PreparedGateRegistration::Upsert(entry) => {
                gates.insert(entry.profile.key.clone(), entry.clone());
            }
            PreparedGateRegistration::Remove(key) => {
                gates.remove(key);
            }
        }
    }

    validate_residents(&residents)?;
    validate_gates(&residents, &gates)?;
    Ok(RegistrationSnapshot {
        id: SnapshotId::new(),
        residents,
        gates,
    })
}

fn validate_residents(
    residents: &BTreeMap<ResidentKey, RegisteredResident>,
) -> Result<(), RegistrationError> {
    let mut machines = BTreeSet::new();
    for entry in residents.values() {
        for definition in &entry.profile.state_machines {
            definition.validate()?;
            if definition.owner != entry.profile.key {
                return Err(RegistrationError::WrongMachineOwner {
                    machine: definition.key.clone(),
                    expected: entry.profile.key.clone(),
                    actual: definition.owner.clone(),
                });
            }
            if !machines.insert(definition.key.clone()) {
                return Err(RegistrationError::DuplicateMachine {
                    machine: definition.key.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_gates(
    residents: &BTreeMap<ResidentKey, RegisteredResident>,
    gates: &BTreeMap<GateKey, RegisteredGate>,
) -> Result<(), RegistrationError> {
    for entry in gates.values() {
        if entry.profile.hooks.is_empty() {
            return Err(RegistrationError::UnboundGate {
                gate: entry.profile.key.clone(),
            });
        }
        for binding in &entry.profile.hooks {
            if !residents.contains_key(&binding.resident) {
                return Err(RegistrationError::UnknownGateResident {
                    gate: entry.profile.key.clone(),
                    resident: binding.resident.clone(),
                });
            }
        }
    }
    Ok(())
}
