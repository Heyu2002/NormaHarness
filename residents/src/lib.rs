//! Concrete Resident implementations built on [`norma_harness`].
//!
//! Shared registration and transport contracts stay in the framework crate.
//! Each module here owns its service-specific state, execution, and protocol.

pub mod chat;
pub mod codex;
pub mod llm;
pub mod media;
pub mod memory;
pub mod tools;
