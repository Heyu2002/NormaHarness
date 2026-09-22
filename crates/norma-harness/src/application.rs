use std::{fmt, sync::Arc};

use crate::{MessageSender, Rdf, RegistrationSender, Rtdf};

/// Composition root for RDF and RTDF.
///
/// Concrete Residents receive only [`RegistrationSender`] and [`MessageSender`]
/// during construction. They never need to retain this application object, RDF,
/// RTDF, or ResidentStore.
pub struct NormaHarness {
    rdf: Arc<Rdf>,
    registration_sender: RegistrationSender,
    rtdf: Rtdf,
}

impl NormaHarness {
    /// Starts the registration and message data flows.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    #[must_use]
    pub fn new() -> Self {
        let (rdf, registration_sender) = Rdf::start();
        let rtdf = Rtdf::start(rdf.clone());
        Self {
            rdf,
            registration_sender,
            rtdf,
        }
    }

    /// Returns the channel-only registration port to inject into a Resident.
    #[must_use]
    pub fn registration_sender(&self) -> RegistrationSender {
        self.registration_sender.clone()
    }

    /// Returns the channel-only message port to inject into a Resident.
    #[must_use]
    pub fn message_sender(&self) -> MessageSender {
        self.rtdf.message_sender()
    }

    /// Read-only application access to the RDF directory.
    #[must_use]
    pub fn rdf(&self) -> &Rdf {
        &self.rdf
    }

    /// Application access to the RTDF entry point.
    #[must_use]
    pub fn rtdf(&self) -> &Rtdf {
        &self.rtdf
    }
}

impl Default for NormaHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for NormaHarness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormaHarness")
            .field("rdf", &self.rdf)
            .field("rtdf", &self.rtdf)
            .finish()
    }
}
