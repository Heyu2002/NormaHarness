//! The Resident architecture runtime.
//!
//! [`RegistrationDataFlow`] (RDF) owns factory registration and publishes
//! immutable registration snapshots. [`RuntimeDataFlow`] (RTDF) creates one
//! Resident/Gate instance set per AgentThread and applies a new snapshot only at
//! a Turn boundary.

mod artifact;
mod context;
mod error;
mod native;
mod rdf;
mod registration;
mod resident_host;
mod runtime;
mod tool_resident;

pub use artifact::{ResidentArtifact, ResidentLimits};
pub use context::AgentThreadSnapshot;
pub use error::{ActivationError, RegistrationError, RuntimeError};
pub use native::{NativeResidentRegistration, Resident, ResidentContext};
pub use rdf::{DiscoveryFailure, DiscoveryReport, RegistrationDataFlow, RegistrationSnapshot};
pub use registration::RegistrationBatch;
pub use runtime::RuntimeDataFlow;
pub use tool_resident::{Tool, ToolResident, ToolResidentBuildError, ToolResidentBuilder};

/// The stable Resident, Gate, flow, and state-machine contracts used by the runtime.
pub use village_harness_protocol as protocol;
