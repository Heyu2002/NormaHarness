use thiserror::Error;

use crate::id::{ResidentInstanceId, ResidentKey};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentifierError {
    #[error("{kind} cannot be empty")]
    Empty { kind: &'static str },
    #[error("{kind} `{value}` contains surrounding whitespace or control characters")]
    Invalid { kind: &'static str, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct GateError {
    message: String,
}

impl GateError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct MailboxError {
    message: String,
}

impl MailboxError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegistrationError {
    #[error("the Resident instance ID counter is exhausted")]
    InstanceIdExhausted,
    #[error("the RDF registration channel is closed")]
    PipelineUnavailable,
    #[error("Resident instance `{instance_id}` is already registered as `{resident}`")]
    InstanceAlreadyRegistered {
        instance_id: ResidentInstanceId,
        resident: ResidentKey,
    },
    #[error(
        "Resident `{resident}` is already registered by instance `{registered_instance}`; instance `{requested_instance}` cannot use the same name"
    )]
    DuplicateResident {
        resident: ResidentKey,
        registered_instance: ResidentInstanceId,
        requested_instance: ResidentInstanceId,
    },
    #[error("Resident instance `{instance_id}` is not registered")]
    UnknownRegistration { instance_id: ResidentInstanceId },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RouteError {
    #[error("the RTDF message channel is closed")]
    PipelineUnavailable,
    #[error("source Resident instance `{instance_id}` is not registered")]
    UnknownSource { instance_id: ResidentInstanceId },
    #[error("target Resident `{resident}` is not registered")]
    UnknownTarget { resident: ResidentKey },
    #[error("outbound Gate for Resident `{resident}` rejected the message: {source}")]
    OutboundGate {
        resident: ResidentKey,
        #[source]
        source: GateError,
    },
    #[error("inbound Gate for Resident `{resident}` rejected the message: {source}")]
    InboundGate {
        resident: ResidentKey,
        #[source]
        source: GateError,
    },
    #[error("mailbox for Resident `{resident}` rejected the message: {source}")]
    TargetMailbox {
        resident: ResidentKey,
        #[source]
        source: MailboxError,
    },
}
