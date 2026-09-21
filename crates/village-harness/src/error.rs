use thiserror::Error;
use village_harness_protocol::{
    GateKey, MachineKey, MessageKind, ResidentKey, StateId, StateMachineError,
};

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RegistrationError {
    #[error("registration batch is empty")]
    EmptyBatch,
    #[error("registration batch contains more than one operation for {kind} {key}")]
    DuplicateOperation { kind: &'static str, key: String },
    #[error("Resident {resident} artifact is invalid: {message}")]
    InvalidResidentArtifact {
        resident: ResidentKey,
        message: String,
    },
    #[error("Resident {resident} imports {module}.{name}; all host imports are forbidden")]
    ResidentImportForbidden {
        resident: ResidentKey,
        module: String,
        name: String,
    },
    #[error("state machine {machine} is owned by {actual}, expected {expected}")]
    WrongMachineOwner {
        machine: MachineKey,
        expected: ResidentKey,
        actual: ResidentKey,
    },
    #[error("state machine {machine} is registered by more than one Resident")]
    DuplicateMachine { machine: MachineKey },
    #[error("Gate {gate} is not mounted to any Resident lifecycle hook")]
    UnboundGate { gate: GateKey },
    #[error("Gate {gate} is mounted to missing Resident {resident}")]
    UnknownGateResident {
        gate: GateKey,
        resident: ResidentKey,
    },
    #[error(transparent)]
    InvalidStateMachine(#[from] StateMachineError),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ActivationError {
    #[error("Resident {resident} could not be initialized: {code}: {message}")]
    ResidentInitialization {
        resident: ResidentKey,
        code: String,
        message: String,
    },
    #[error("Gate {gate} could not be initialized: {code}: {message}")]
    GateInitialization {
        gate: GateKey,
        code: String,
        message: String,
    },
    #[error(
        "state machine {machine} cannot carry state {state} into the new registration snapshot"
    )]
    IncompatibleState { machine: MachineKey, state: StateId },
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("AgentThread has no usable registration snapshot: {0}")]
    Activation(#[from] ActivationError),
    #[error("target Resident {0} is not registered in this Turn")]
    UnknownTarget(ResidentKey),
    #[error("Resident {resident} does not accept message kind {kind}")]
    UnsupportedMessage {
        resident: ResidentKey,
        kind: MessageKind,
    },
    #[error("Resident {resident} failed [{code}]: {message}")]
    ResidentFailure {
        resident: ResidentKey,
        code: String,
        message: String,
    },
    #[error("Gate {gate} failed [{code}]: {message}")]
    GateFailure {
        gate: GateKey,
        code: String,
        message: String,
    },
    #[error(
        "Resident {resident} attempted to mutate state machine {machine}, which it does not own"
    )]
    ForeignStateMachine {
        resident: ResidentKey,
        machine: MachineKey,
    },
    #[error(transparent)]
    StateTransition(#[from] StateMachineError),
    #[error("packet exceeded the Turn hop limit of {limit}")]
    HopLimitExceeded { limit: u32 },
    #[error("Turn exceeded the packet limit of {limit}")]
    PacketLimitExceeded { limit: usize },
}
