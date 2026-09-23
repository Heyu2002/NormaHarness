//! Codex-specific message names retain compatibility with direct callers.
//! The payload shape is the shared LLM turn contract.

pub use crate::llm::{
    LlmTurnRequest as CodexTurnRequest, LlmTurnResult as CodexTurnResult,
    LlmTurnStatus as CodexTurnStatus,
};

pub const TURN_REQUEST_KIND: &str = "codex.turn.request";
pub const TURN_RESULT_KIND: &str = "codex.turn.result";
