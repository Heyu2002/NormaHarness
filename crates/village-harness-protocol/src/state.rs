use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{EventId, MachineKey, ResidentKey, StateId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransitionRule {
    pub from: StateId,
    pub event: EventId,
    pub to: StateId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StateMachineDefinition {
    pub key: MachineKey,
    pub owner: ResidentKey,
    pub initial_state: StateId,
    pub states: BTreeSet<StateId>,
    pub transitions: Vec<TransitionRule>,
}

impl StateMachineDefinition {
    pub fn validate(&self) -> Result<(), StateMachineError> {
        if !self.states.contains(&self.initial_state) {
            return Err(StateMachineError::UnknownInitialState {
                machine: self.key.clone(),
                state: self.initial_state.clone(),
            });
        }

        let mut seen = BTreeMap::new();
        for rule in &self.transitions {
            if !self.states.contains(&rule.from) {
                return Err(StateMachineError::UnknownState {
                    machine: self.key.clone(),
                    state: rule.from.clone(),
                });
            }
            if !self.states.contains(&rule.to) {
                return Err(StateMachineError::UnknownState {
                    machine: self.key.clone(),
                    state: rule.to.clone(),
                });
            }

            let transition_key = (rule.from.clone(), rule.event.clone());
            if seen.insert(transition_key, rule.to.clone()).is_some() {
                return Err(StateMachineError::AmbiguousTransition {
                    machine: self.key.clone(),
                    state: rule.from.clone(),
                    event: rule.event.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn next_state(
        &self,
        current: &StateId,
        event: &EventId,
    ) -> Result<StateId, StateMachineError> {
        self.transitions
            .iter()
            .find(|rule| &rule.from == current && &rule.event == event)
            .map(|rule| rule.to.clone())
            .ok_or_else(|| StateMachineError::InvalidTransition {
                machine: self.key.clone(),
                state: current.clone(),
                event: event.clone(),
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StateEvent {
    pub machine: MachineKey,
    pub event: EventId,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StateMachineError {
    #[error("state machine {machine} uses unknown initial state {state}")]
    UnknownInitialState { machine: MachineKey, state: StateId },
    #[error("state machine {machine} references unknown state {state}")]
    UnknownState { machine: MachineKey, state: StateId },
    #[error("state machine {machine} has more than one transition from {state} on {event}")]
    AmbiguousTransition {
        machine: MachineKey,
        state: StateId,
        event: EventId,
    },
    #[error("state machine {machine} cannot handle {event} from {state}")]
    InvalidTransition {
        machine: MachineKey,
        state: StateId,
        event: EventId,
    },
}
