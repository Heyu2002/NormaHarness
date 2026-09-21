use serde::{Deserialize, Serialize};

/// Stable interception points around one Resident delivery.
///
/// These points belong to the Resident lifecycle. Gates are one kind of
/// component that may be mounted onto them; the lifecycle does not depend on
/// Gate implementations.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidentHookPoint {
    BeforeReceive,
    AfterExecute,
    BeforeCommit,
    OnFailure,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidentFailureStage {
    BeforeReceive,
    Execute,
    Commit,
}
