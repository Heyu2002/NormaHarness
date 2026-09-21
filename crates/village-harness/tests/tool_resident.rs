use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex as StdMutex},
};

use async_trait::async_trait;
use serde_json::{Value, json};
use village_harness::{
    NativeResidentRegistration, RegistrationBatch, RegistrationDataFlow, Resident, ResidentContext,
    RuntimeDataFlow, Tool, ToolResident, ToolResidentBuildError,
};
use village_harness_protocol::{
    BeforeReceiveAction, FailureResolution, FlowPacket, FlowTarget, Gate, GateContext, GateError,
    GateFactory, GateHookBinding, GateInitContext, GateKey, GateProfile, MessageKind,
    ResidentError, ResidentFailureStage, ResidentHookPoint, ResidentKey, ResidentProfile,
    RuntimeFailure, TOOL_INVOKE_REQUEST_KIND, TOOL_LIST_REQUEST_KIND, TOOL_RESULT_KIND, ThreadId,
    ToolCatalog, ToolDescriptor, ToolError, ToolInvokeRequest, ToolKey, ToolListRequest,
    ToolOutcome, ToolResult,
};

fn resident_key(value: &str) -> ResidentKey {
    ResidentKey::new(value).unwrap()
}

fn tool_key(value: &str) -> ToolKey {
    ToolKey::new(value).unwrap()
}

fn descriptor(value: &str) -> ToolDescriptor {
    ToolDescriptor::new(
        tool_key(value),
        format!("{value} test tool"),
        json!({ "type": "object" }),
    )
}

struct UpperTool;

#[async_trait]
impl Tool for UpperTool {
    async fn invoke(&mut self, arguments: Value) -> Result<Value, ToolError> {
        let text = arguments["text"].as_str().unwrap_or_default();
        if text.is_empty() {
            return Err(ToolError::new("EMPTY_TEXT", "text must not be empty"));
        }
        Ok(json!({ "text": text.to_uppercase() }))
    }
}

struct PositiveTool;

#[async_trait]
impl Tool for PositiveTool {
    async fn invoke(&mut self, arguments: Value) -> Result<Value, ToolError> {
        let value = arguments["value"].as_i64().unwrap_or_default();
        if value <= 0 {
            return Err(ToolError::new(
                "POSITIVE_REQUIRED",
                "value must be positive",
            ));
        }
        Ok(json!({ "value": value }))
    }
}

#[derive(Default)]
struct CounterTool {
    count: u64,
}

#[async_trait]
impl Tool for CounterTool {
    async fn invoke(&mut self, _arguments: Value) -> Result<Value, ToolError> {
        self.count += 1;
        Ok(json!({ "count": self.count }))
    }
}

fn tool_resident_registration() -> village_harness::NativeResidentRegistration {
    ToolResident::builder(resident_key("system.tools"))
        .tool(descriptor("upper"), || UpperTool)
        .tool(descriptor("positive"), || PositiveTool)
        .build()
        .unwrap()
}

struct ToolCallerResident;

#[async_trait]
impl Resident for ToolCallerResident {
    async fn handle(&mut self, context: &mut ResidentContext) -> Result<(), ResidentError> {
        match context.message().kind.as_str() {
            "caller.start" => context.send(
                resident_key("system.tools"),
                ToolInvokeRequest::new(
                    tool_key("upper"),
                    json!({ "text": "village" }),
                    FlowTarget::Resident(resident_key("caller")),
                )
                .into_message(),
            ),
            TOOL_RESULT_KIND => context.reply(context.message().clone()),
            kind => {
                return Err(ResidentError::new(
                    "CALLER_PROTOCOL",
                    format!("unsupported caller message kind {kind}"),
                ));
            }
        }
        Ok(())
    }
}

fn caller_registration() -> NativeResidentRegistration {
    NativeResidentRegistration::new(
        ResidentProfile {
            key: resident_key("caller"),
            accepted_messages: ["caller.start", TOOL_RESULT_KIND]
                .into_iter()
                .map(|kind| MessageKind::new(kind).unwrap())
                .collect(),
            provided_capabilities: BTreeSet::new(),
            state_machines: Vec::new(),
        },
        || ToolCallerResident,
    )
}

struct ToolAuditGateFactory {
    message_kinds: Arc<StdMutex<Vec<String>>>,
    failures: Arc<StdMutex<Vec<ResidentFailureStage>>>,
}

#[async_trait]
impl GateFactory for ToolAuditGateFactory {
    fn profile(&self) -> GateProfile {
        GateProfile {
            key: GateKey::new("tool-audit").unwrap(),
            order: 0,
            hooks: BTreeSet::from([
                GateHookBinding {
                    resident: resident_key("system.tools"),
                    point: ResidentHookPoint::BeforeReceive,
                },
                GateHookBinding {
                    resident: resident_key("system.tools"),
                    point: ResidentHookPoint::OnFailure,
                },
            ]),
        }
    }

    async fn instantiate(&self, _context: GateInitContext) -> Result<Box<dyn Gate>, GateError> {
        Ok(Box::new(ToolAuditGate {
            message_kinds: self.message_kinds.clone(),
            failures: self.failures.clone(),
        }))
    }
}

struct ToolAuditGate {
    message_kinds: Arc<StdMutex<Vec<String>>>,
    failures: Arc<StdMutex<Vec<ResidentFailureStage>>>,
}

#[async_trait]
impl Gate for ToolAuditGate {
    async fn before_receive(
        &mut self,
        _context: GateContext,
        packet: &FlowPacket,
    ) -> Result<BeforeReceiveAction, GateError> {
        self.message_kinds
            .lock()
            .unwrap()
            .push(packet.message.kind.to_string());
        Ok(BeforeReceiveAction::Continue(packet.message.clone()))
    }

    async fn on_failure(
        &mut self,
        _context: GateContext,
        failure: RuntimeFailure,
    ) -> Result<FailureResolution, GateError> {
        self.failures.lock().unwrap().push(failure.stage);
        Ok(FailureResolution::Propagate)
    }
}

#[tokio::test]
async fn tool_catalog_stays_inside_the_tool_resident() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    rdf.submit(RegistrationBatch::new().upsert_native_resident(tool_resident_registration()))
        .await
        .unwrap();

    let discovery = rdf.discover().await;
    assert_eq!(discovery.snapshot.resident_count(), 1);
    let directory_json = serde_json::to_string(&discovery.snapshot.resident_directory()).unwrap();
    assert!(!directory_json.contains("upper"));
    assert!(!directory_json.contains("positive"));

    let output = rtdf
        .run_turn(
            ThreadId::new("thread-a").unwrap(),
            resident_key("system.tools"),
            ToolListRequest::new(FlowTarget::Return).into_message(),
        )
        .await
        .unwrap();
    let catalog: ToolCatalog = serde_json::from_value(output.returned[0].payload.clone()).unwrap();
    assert_eq!(
        catalog
            .tools
            .iter()
            .map(|tool| tool.key.as_str())
            .collect::<Vec<_>>(),
        vec!["positive", "upper"]
    );
}

#[tokio::test]
async fn one_resident_gate_wraps_every_tool_and_tools_own_business_errors() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let message_kinds = Arc::new(StdMutex::new(Vec::new()));
    let failures = Arc::new(StdMutex::new(Vec::new()));
    rdf.submit(
        RegistrationBatch::new()
            .upsert_native_resident(tool_resident_registration())
            .upsert_gate(Arc::new(ToolAuditGateFactory {
                message_kinds: message_kinds.clone(),
                failures: failures.clone(),
            })),
    )
    .await
    .unwrap();

    let thread = ThreadId::new("thread-a").unwrap();
    let upper = rtdf
        .run_turn(
            thread.clone(),
            resident_key("system.tools"),
            ToolInvokeRequest::new(tool_key("upper"), json!({ "text": "" }), FlowTarget::Return)
                .into_message(),
        )
        .await
        .unwrap();
    let positive = rtdf
        .run_turn(
            thread,
            resident_key("system.tools"),
            ToolInvokeRequest::new(
                tool_key("positive"),
                json!({ "value": 0 }),
                FlowTarget::Return,
            )
            .into_message(),
        )
        .await
        .unwrap();

    let upper: ToolResult = serde_json::from_value(upper.returned[0].payload.clone()).unwrap();
    let positive: ToolResult =
        serde_json::from_value(positive.returned[0].payload.clone()).unwrap();
    assert!(matches!(
        upper.outcome,
        ToolOutcome::Error(ref error) if error.code == "EMPTY_TEXT"
    ));
    assert!(matches!(
        positive.outcome,
        ToolOutcome::Error(ref error) if error.code == "POSITIVE_REQUIRED"
    ));
    assert_eq!(
        *message_kinds.lock().unwrap(),
        vec![TOOL_INVOKE_REQUEST_KIND, TOOL_INVOKE_REQUEST_KIND]
    );
    assert!(failures.lock().unwrap().is_empty());
}

#[tokio::test]
async fn resident_uses_tools_by_message_without_registering_itself_with_tool_resident() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    rdf.submit(
        RegistrationBatch::new()
            .upsert_native_resident(caller_registration())
            .upsert_native_resident(tool_resident_registration()),
    )
    .await
    .unwrap();

    let output = rtdf
        .run_turn(
            ThreadId::new("thread-a").unwrap(),
            resident_key("caller"),
            village_harness_protocol::FlowMessage::new(
                MessageKind::new("caller.start").unwrap(),
                Value::Null,
            ),
        )
        .await
        .unwrap();

    assert_eq!(output.processed_packets, 3);
    let result: ToolResult = serde_json::from_value(output.returned[0].payload.clone()).unwrap();
    assert!(matches!(
        result.outcome,
        ToolOutcome::Success(ref value) if value == &json!({ "text": "VILLAGE" })
    ));
}

#[tokio::test]
async fn tool_instances_are_isolated_per_agent_thread() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let registration = ToolResident::builder(resident_key("system.tools"))
        .tool(descriptor("counter"), CounterTool::default)
        .build()
        .unwrap();
    rdf.submit(RegistrationBatch::new().upsert_native_resident(registration))
        .await
        .unwrap();

    let invoke = |thread: &str| {
        rtdf.run_turn(
            ThreadId::new(thread).unwrap(),
            resident_key("system.tools"),
            ToolInvokeRequest::new(tool_key("counter"), Value::Null, FlowTarget::Return)
                .into_message(),
        )
    };

    let thread_a_first = invoke("thread-a").await.unwrap();
    let thread_a_second = invoke("thread-a").await.unwrap();
    let thread_b_first = invoke("thread-b").await.unwrap();

    let count = |output: &village_harness_protocol::TurnOutput| {
        let result: ToolResult =
            serde_json::from_value(output.returned[0].payload.clone()).unwrap();
        match result.outcome {
            ToolOutcome::Success(value) => value["count"].as_u64().unwrap(),
            outcome => panic!("expected a successful counter result, got {outcome:?}"),
        }
    };
    assert_eq!(count(&thread_a_first), 1);
    assert_eq!(count(&thread_a_second), 2);
    assert_eq!(count(&thread_b_first), 1);
}

#[test]
fn duplicate_tool_names_are_rejected_by_the_tool_resident_not_rdf() {
    let error = ToolResident::builder(resident_key("system.tools"))
        .tool(descriptor("upper"), || UpperTool)
        .tool(descriptor("upper"), || UpperTool)
        .build()
        .unwrap_err();
    assert_eq!(
        error,
        ToolResidentBuildError::DuplicateTool(tool_key("upper"))
    );
}

#[test]
fn tool_resident_only_declares_resident_level_message_kinds() {
    let registration = tool_resident_registration();
    assert!(
        registration
            .profile()
            .accepted_messages
            .contains(&village_harness_protocol::MessageKind::new(TOOL_LIST_REQUEST_KIND).unwrap())
    );
    assert!(
        registration.profile().accepted_messages.contains(
            &village_harness_protocol::MessageKind::new(TOOL_INVOKE_REQUEST_KIND).unwrap()
        )
    );
    assert_eq!(registration.profile().provided_capabilities.len(), 1);
}
