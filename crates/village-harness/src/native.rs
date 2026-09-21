use std::{fmt, sync::Arc};

use async_trait::async_trait;
use village_harness_protocol::{
    Emission, FlowMessage, FlowPacket, ResidentContextSnapshot, ResidentEffect, ResidentError,
    ResidentInvocation, ResidentKey, ResidentProfile, StateEvent,
};

const DEFAULT_NATIVE_MAILBOX_CAPACITY: usize = 16;

/// The business interface implemented by a trusted in-process Resident.
///
/// Layout owns the instance and invokes this method inside the same fixed
/// lifecycle used by every other Resident driver. Implementations receive only
/// a data context; RDF, RTDF, peer instances, and mailboxes are never exposed.
#[async_trait]
pub trait Resident: Send + 'static {
    async fn handle(&mut self, context: &mut ResidentContext) -> Result<(), ResidentError>;

    async fn shutdown(&mut self) -> Result<(), ResidentError> {
        Ok(())
    }
}

/// The capability-limited context supplied to a trusted native Resident.
///
/// `send` and `reply` only stage Emissions. RTDF retains ownership of actual
/// packet creation and delivery after the Resident returns successfully.
#[derive(Debug)]
pub struct ResidentContext {
    invocation: ResidentInvocation,
    effect: ResidentEffect,
}

impl ResidentContext {
    pub(crate) fn new(invocation: ResidentInvocation) -> Self {
        Self {
            invocation,
            effect: ResidentEffect::default(),
        }
    }

    pub fn packet(&self) -> &FlowPacket {
        &self.invocation.packet
    }

    pub fn message(&self) -> &FlowMessage {
        &self.invocation.packet.message
    }

    pub fn snapshot(&self) -> &ResidentContextSnapshot {
        &self.invocation.context
    }

    pub fn send(&mut self, target: ResidentKey, message: FlowMessage) {
        self.effect.emissions.push(Emission::to(target, message));
    }

    pub fn reply(&mut self, message: FlowMessage) {
        self.effect
            .emissions
            .push(Emission::return_to_caller(message));
    }

    pub fn emit(&mut self, emission: Emission) {
        self.effect.emissions.push(emission);
    }

    pub fn state_event(&mut self, event: StateEvent) {
        self.effect.state_events.push(event);
    }

    pub(crate) fn into_effect(self) -> ResidentEffect {
        self.effect
    }
}

trait NativeResidentFactory: Send + Sync {
    fn create(&self) -> Box<dyn Resident>;
}

impl<F, R> NativeResidentFactory for F
where
    F: Fn() -> R + Send + Sync,
    R: Resident,
{
    fn create(&self) -> Box<dyn Resident> {
        Box::new(self())
    }
}

/// Registration data for a trusted native Resident.
///
/// The factory is retained only by Layout and is invoked separately for each
/// AgentThread. A live Resident instance is never stored in RDF or shared
/// between threads.
#[derive(Clone)]
pub struct NativeResidentRegistration {
    profile: ResidentProfile,
    mailbox_capacity: usize,
    factory: Arc<dyn NativeResidentFactory>,
}

impl NativeResidentRegistration {
    pub fn new<F, R>(profile: ResidentProfile, factory: F) -> Self
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: Resident,
    {
        Self {
            profile,
            mailbox_capacity: DEFAULT_NATIVE_MAILBOX_CAPACITY,
            factory: Arc::new(factory),
        }
    }

    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity;
        self
    }

    pub fn profile(&self) -> &ResidentProfile {
        &self.profile
    }

    pub fn mailbox_capacity(&self) -> usize {
        self.mailbox_capacity
    }

    pub(crate) fn instantiate(&self) -> Box<dyn Resident> {
        self.factory.create()
    }
}

impl fmt::Debug for NativeResidentRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeResidentRegistration")
            .field("profile", &self.profile)
            .field("mailbox_capacity", &self.mailbox_capacity)
            .finish_non_exhaustive()
    }
}
