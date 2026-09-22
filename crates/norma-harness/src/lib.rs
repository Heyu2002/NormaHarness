//! Norma Harness exposes two narrow, channel-driven data flows:
//!
//! - [`NormaHarness`] wires the data flows and supplies injection ports.
//! - [`Rdf`] atomically registers and owns concrete Resident instances.
//! - [`Rtdf`] moves one message between registered Residents.
//!
//! Each Resident retains control of its execution, state, retries, cancellation,
//! fallback, and shutdown boundary.

mod application;
mod error;
mod gate;
mod id;
mod mailbox;
mod message;
mod rdf;
mod resident;
mod resident_store;
mod rtdf;

pub use application::NormaHarness;
pub use error::{GateError, IdentifierError, MailboxError, RegistrationError, RouteError};
pub use gate::{Gate, GateContext};
pub use id::{CapabilityKey, MessageKind, ResidentInstanceId, ResidentKey};
pub use mailbox::{Mailbox, MailboxAddress};
pub use message::{FlowMessage, RoutedMessage};
pub use rdf::{Rdf, RegistrationSender};
pub use resident::{
    RegistrationNoticeError, RegistrationNoticeFailure, RegistrationReceipt, Resident,
    ResidentDescriptor, ResidentEvent,
};
pub use rtdf::{MessageSender, Rtdf};
