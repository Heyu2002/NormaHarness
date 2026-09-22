use std::{collections::BTreeSet, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    CapabilityKey, Gate, MailboxAddress, MailboxError, ResidentInstanceId, ResidentKey,
    RoutedMessage,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentDescriptor {
    key: ResidentKey,
    capabilities: BTreeSet<CapabilityKey>,
}

impl ResidentDescriptor {
    #[must_use]
    pub fn new(key: ResidentKey, capabilities: impl IntoIterator<Item = CapabilityKey>) -> Self {
        Self {
            key,
            capabilities: capabilities.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn key(&self) -> &ResidentKey {
        &self.key
    }

    #[must_use]
    pub fn capabilities(&self) -> &BTreeSet<CapabilityKey> {
        &self.capabilities
    }

    #[must_use]
    pub fn provides(&self, capability: &CapabilityKey) -> bool {
        self.capabilities.contains(capability)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResidentEvent {
    ResidentRegistered(ResidentDescriptor),
    Message(RoutedMessage),
}

/// The narrow surface that the application layer stores and the data flows inspect.
///
/// Execution, state, retries, cancellation, fallback, and shutdown remain private
/// to the concrete Resident implementation. A Resident's descriptor must remain
/// stable from successful registration until unregistration.
pub trait Resident: Send + Sync + 'static {
    fn descriptor(&self) -> ResidentDescriptor;

    fn mailbox(&self) -> MailboxAddress;

    fn outbound_gate(&self) -> Option<Arc<dyn Gate>> {
        None
    }

    fn inbound_gate(&self) -> Option<Arc<dyn Gate>> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationNoticeError {
    Mailbox(MailboxError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationNoticeFailure {
    resident: ResidentKey,
    error: RegistrationNoticeError,
}

impl RegistrationNoticeFailure {
    pub(crate) fn new(resident: ResidentKey, error: RegistrationNoticeError) -> Self {
        Self { resident, error }
    }

    #[must_use]
    pub fn resident(&self) -> &ResidentKey {
        &self.resident
    }

    #[must_use]
    pub fn error(&self) -> &RegistrationNoticeError {
        &self.error
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationReceipt {
    instance_id: ResidentInstanceId,
    existing_residents: Vec<ResidentDescriptor>,
    notice_failures: Vec<RegistrationNoticeFailure>,
}

impl RegistrationReceipt {
    pub(crate) fn new(
        instance_id: ResidentInstanceId,
        existing_residents: Vec<ResidentDescriptor>,
        notice_failures: Vec<RegistrationNoticeFailure>,
    ) -> Self {
        Self {
            instance_id,
            existing_residents,
            notice_failures,
        }
    }

    #[must_use]
    pub fn instance_id(&self) -> ResidentInstanceId {
        self.instance_id
    }

    #[must_use]
    pub fn existing_residents(&self) -> &[ResidentDescriptor] {
        &self.existing_residents
    }

    #[must_use]
    pub fn notice_failures(&self) -> &[RegistrationNoticeFailure] {
        &self.notice_failures
    }
}
