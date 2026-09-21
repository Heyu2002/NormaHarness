use std::sync::Arc;

use village_harness_protocol::ResidentProfile;

/// Resource limits enforced independently for every WASM Resident instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidentLimits {
    pub max_memory_bytes: usize,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub fuel_per_delivery: u64,
    pub mailbox_capacity: usize,
}

impl Default for ResidentLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 16 * 1024 * 1024,
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
            fuel_per_delivery: 10_000_000,
            mailbox_capacity: 16,
        }
    }
}

/// A data-only WASM Resident registration artifact.
///
/// It contains Wasm bytes and declarative metadata, never a live Rust object,
/// closure, factory, Store, or reference to another Resident.
#[derive(Clone)]
pub struct ResidentArtifact {
    profile: ResidentProfile,
    wasm: Arc<[u8]>,
    limits: ResidentLimits,
}

impl ResidentArtifact {
    pub fn new(profile: ResidentProfile, wasm: impl Into<Arc<[u8]>>) -> Self {
        Self {
            profile,
            wasm: wasm.into(),
            limits: ResidentLimits::default(),
        }
    }

    pub fn with_limits(mut self, limits: ResidentLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn profile(&self) -> &ResidentProfile {
        &self.profile
    }

    pub fn limits(&self) -> ResidentLimits {
        self.limits
    }

    pub(crate) fn wasm(&self) -> &[u8] {
        &self.wasm
    }
}

impl std::fmt::Debug for ResidentArtifact {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResidentArtifact")
            .field("profile", &self.profile)
            .field("wasm_bytes", &self.wasm.len())
            .field("limits", &self.limits)
            .finish()
    }
}
