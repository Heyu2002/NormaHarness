use std::sync::Arc;

use crate::{MailboxError, ResidentEvent};

/// A Resident-owned address that accepts registration notices and routed messages.
///
/// `deliver` must only enqueue the event and return. It must not execute Resident
/// business logic or synchronously call back into RDF/RTDF.
pub trait Mailbox: Send + Sync + 'static {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError>;
}

pub type MailboxAddress = Arc<dyn Mailbox>;
