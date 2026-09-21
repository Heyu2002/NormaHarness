use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use serde_json::json;
use village_harness::{
    NativeResidentRegistration, RegistrationBatch, RegistrationDataFlow, Resident, ResidentContext,
    RuntimeDataFlow,
};
use village_harness_protocol::{
    FlowMessage, MessageKind, ResidentError, ResidentKey, ResidentProfile, ThreadId,
};

struct EchoResident;

#[async_trait]
impl Resident for EchoResident {
    async fn handle(&mut self, context: &mut ResidentContext) -> Result<(), ResidentError> {
        context.reply(FlowMessage::new(
            MessageKind::new("echo.result").unwrap(),
            context.message().payload.clone(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn ordinary_native_resident_runs_without_a_tool_resident() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let profile = ResidentProfile {
        key: ResidentKey::new("echo").unwrap(),
        accepted_messages: BTreeSet::from([MessageKind::new("echo.request").unwrap()]),
        provided_capabilities: BTreeSet::new(),
        state_machines: Vec::new(),
    };
    rdf.submit(
        RegistrationBatch::new()
            .upsert_native_resident(NativeResidentRegistration::new(profile, || EchoResident)),
    )
    .await
    .unwrap();

    let output = rtdf
        .run_turn(
            ThreadId::new("thread-a").unwrap(),
            ResidentKey::new("echo").unwrap(),
            FlowMessage::new(
                MessageKind::new("echo.request").unwrap(),
                json!({ "value": 42 }),
            ),
        )
        .await
        .unwrap();

    assert_eq!(output.returned[0].payload, json!({ "value": 42 }));
    let snapshot = rdf.active_snapshot().await;
    assert_eq!(snapshot.resident_count(), 1);
    assert_eq!(snapshot.gate_count(), 0);
}
