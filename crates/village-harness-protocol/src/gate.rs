use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    FlowMessage, FlowPacket, FlowTarget, GateKey, MachineKey, ResidentEffect, ResidentFailureStage,
    ResidentHookPoint, ResidentKey, SnapshotId, StateId, ThreadId,
};

#[async_trait]
pub trait LayoutAccess: Send + Sync {
    fn thread_id(&self) -> ThreadId;

    async fn active_snapshot(&self) -> Option<SnapshotId>;

    async fn current_state(&self, machine: &MachineKey) -> Option<StateId>;
}

/// Host capability supplied only to trusted native Gates.
#[derive(Clone)]
pub struct GateInitContext {
    pub layout: Arc<dyn LayoutAccess>,
}

impl std::fmt::Debug for GateInitContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GateInitContext")
            .field("thread_id", &self.layout.thread_id())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GateHookBinding {
    pub resident: ResidentKey,
    pub point: ResidentHookPoint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GateProfile {
    pub key: GateKey,
    pub order: i32,
    pub hooks: BTreeSet<GateHookBinding>,
}

#[derive(Clone)]
pub struct GateContext {
    pub layout: Arc<dyn LayoutAccess>,
    pub resident: ResidentKey,
}

impl std::fmt::Debug for GateContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GateContext")
            .field("thread_id", &self.layout.thread_id())
            .field("resident", &self.resident)
            .finish_non_exhaustive()
    }
}

/// A Gate may rewrite only the message at the receive boundary.
///
/// RTDF retains ownership of packet identity, correlation, and hop metadata.
/// Redirect is explicit so RTDF can run the target Resident's own
/// `BeforeReceive` chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", content = "value", rename_all = "snake_case")]
pub enum BeforeReceiveAction {
    Continue(FlowMessage),
    Drop,
    Redirect(FlowTarget),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeFailure {
    pub resident: ResidentKey,
    pub stage: ResidentFailureStage,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", content = "packet", rename_all = "snake_case")]
pub enum FailureResolution {
    Propagate,
    Drop,
    Redirect(FlowPacket),
}

#[async_trait]
pub trait GateFactory: Send + Sync {
    fn profile(&self) -> GateProfile;

    async fn instantiate(&self, context: GateInitContext) -> Result<Box<dyn Gate>, GateError>;
}

#[async_trait]
pub trait Gate: Send {
    async fn before_receive(
        &mut self,
        _context: GateContext,
        packet: &FlowPacket,
    ) -> Result<BeforeReceiveAction, GateError> {
        Ok(BeforeReceiveAction::Continue(packet.message.clone()))
    }

    async fn after_execute(
        &mut self,
        _context: GateContext,
        _packet: &FlowPacket,
        effect: ResidentEffect,
    ) -> Result<ResidentEffect, GateError> {
        Ok(effect)
    }

    async fn before_commit(
        &mut self,
        _context: GateContext,
        _packet: &FlowPacket,
        effect: ResidentEffect,
    ) -> Result<ResidentEffect, GateError> {
        Ok(effect)
    }

    async fn on_failure(
        &mut self,
        _context: GateContext,
        _failure: RuntimeFailure,
    ) -> Result<FailureResolution, GateError> {
        Ok(FailureResolution::Propagate)
    }

    async fn shutdown(&mut self) -> Result<(), GateError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq, Serialize, Deserialize)]
#[error("gate failed [{code}]: {message}")]
pub struct GateError {
    pub code: String,
    pub message: String,
}

impl GateError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}
