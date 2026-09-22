use async_trait::async_trait;

use crate::{FlowMessage, GateError, ResidentKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateContext {
    source: ResidentKey,
    target: ResidentKey,
}

impl GateContext {
    #[must_use]
    pub fn new(source: ResidentKey, target: ResidentKey) -> Self {
        Self { source, target }
    }

    #[must_use]
    pub fn source(&self) -> &ResidentKey {
        &self.source
    }

    #[must_use]
    pub fn target(&self) -> &ResidentKey {
        &self.target
    }
}

/// A single transport boundary owned by a Resident.
///
/// A Gate may validate or transform a message. Resident lifecycle hooks and
/// business execution do not belong here.
#[async_trait]
pub trait Gate: Send + Sync + 'static {
    async fn pass(
        &self,
        context: &GateContext,
        message: FlowMessage,
    ) -> Result<FlowMessage, GateError>;
}
