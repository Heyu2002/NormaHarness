use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;
use village_harness_protocol::{
    LayoutAccess, MachineKey, SnapshotId, StateId, StateMachineDefinition, ThreadId,
};

use crate::ActivationError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentThreadSnapshot {
    pub thread_id: ThreadId,
    pub active_snapshot: Option<SnapshotId>,
    pub machine_states: BTreeMap<MachineKey, StateId>,
}

#[derive(Debug)]
pub(crate) struct AgentThreadContext {
    pub(crate) thread_id: ThreadId,
    pub(crate) active_snapshot: Option<SnapshotId>,
    pub(crate) machine_states: BTreeMap<MachineKey, StateId>,
}

impl AgentThreadContext {
    pub fn new(thread_id: ThreadId) -> Self {
        Self {
            thread_id,
            active_snapshot: None,
            machine_states: BTreeMap::new(),
        }
    }

    pub fn stage_states(
        &self,
        definitions: &BTreeMap<MachineKey, StateMachineDefinition>,
    ) -> Result<BTreeMap<MachineKey, StateId>, ActivationError> {
        definitions
            .iter()
            .map(|(machine, definition)| {
                let state = match self.machine_states.get(machine) {
                    Some(state) if definition.states.contains(state) => state.clone(),
                    Some(state) => {
                        return Err(ActivationError::IncompatibleState {
                            machine: machine.clone(),
                            state: state.clone(),
                        });
                    }
                    None => definition.initial_state.clone(),
                };
                Ok((machine.clone(), state))
            })
            .collect()
    }

    pub fn activate(
        &mut self,
        snapshot: SnapshotId,
        machine_states: BTreeMap<MachineKey, StateId>,
    ) {
        self.active_snapshot = Some(snapshot);
        self.machine_states = machine_states;
    }

    pub fn snapshot(&self) -> AgentThreadSnapshot {
        AgentThreadSnapshot {
            thread_id: self.thread_id.clone(),
            active_snapshot: self.active_snapshot,
            machine_states: self.machine_states.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ThreadLayoutAccess {
    thread_id: ThreadId,
    context: Arc<RwLock<AgentThreadContext>>,
}

impl ThreadLayoutAccess {
    pub fn new(thread_id: ThreadId, context: Arc<RwLock<AgentThreadContext>>) -> Self {
        Self { thread_id, context }
    }
}

#[async_trait]
impl LayoutAccess for ThreadLayoutAccess {
    fn thread_id(&self) -> ThreadId {
        self.thread_id.clone()
    }

    async fn active_snapshot(&self) -> Option<SnapshotId> {
        self.context.read().await.active_snapshot
    }

    async fn current_state(&self, machine: &MachineKey) -> Option<StateId> {
        self.context
            .read()
            .await
            .machine_states
            .get(machine)
            .cloned()
    }
}
