//! Data-only contracts shared by Layout, isolated Residents, and trusted Gates.

mod flow;
mod gate;
mod id;
mod lifecycle;
mod resident;
mod state;
mod tool;

pub use flow::{
    Emission, FlowMessage, FlowPacket, FlowTarget, ResidentEffect, TurnLimits, TurnOutput,
};
pub use gate::{
    BeforeReceiveAction, FailureResolution, Gate, GateContext, GateError, GateFactory,
    GateHookBinding, GateInitContext, GateProfile, LayoutAccess, RuntimeFailure,
};
pub use id::{
    BatchId, CapabilityKey, EventId, ExposureId, GateKey, MachineKey, MessageKind, ResidentKey,
    SnapshotId, StateId, ThreadId, ToolKey,
};
pub use lifecycle::{ResidentFailureStage, ResidentHookPoint};
pub use resident::{
    ResidentContextSnapshot, ResidentDescriptor, ResidentDirectorySnapshot, ResidentError,
    ResidentInvocation, ResidentProfile, ResidentResponse,
};
pub use state::{StateEvent, StateMachineDefinition, StateMachineError, TransitionRule};
pub use tool::{
    TOOL_CANCEL_REQUEST_KIND, TOOL_CATALOG_KIND, TOOL_INVOKE_REQUEST_KIND, TOOL_LIST_REQUEST_KIND,
    TOOL_RESULT_KIND, ToolCancelRequest, ToolCatalog, ToolDescriptor, ToolError, ToolInvokeRequest,
    ToolListRequest, ToolOutcome, ToolResult,
};
