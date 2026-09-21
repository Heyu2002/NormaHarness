use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex as StdMutex},
};

use async_trait::async_trait;
use serde_json::{Value, json};
use village_harness::{
    ActivationError, RegistrationBatch, RegistrationDataFlow, RegistrationError, ResidentArtifact,
    ResidentLimits, RuntimeDataFlow, RuntimeError,
};
use village_harness_protocol::{
    BeforeReceiveAction, CapabilityKey, Emission, EventId, FailureResolution, FlowMessage,
    FlowPacket, Gate, GateContext, GateError, GateFactory, GateHookBinding, GateInitContext,
    GateProfile, MachineKey, MessageKind, ResidentEffect, ResidentHookPoint, ResidentKey,
    ResidentProfile, ResidentResponse, RuntimeFailure, StateEvent, StateId, StateMachineDefinition,
    ThreadId, TransitionRule,
};

fn resident_key(value: &str) -> ResidentKey {
    ResidentKey::new(value).expect("test Resident key is valid")
}

fn gate_key(value: &str) -> village_harness_protocol::GateKey {
    village_harness_protocol::GateKey::new(value).expect("test Gate key is valid")
}

fn message_kind(value: &str) -> MessageKind {
    MessageKind::new(value).expect("test message kind is valid")
}

fn thread_id(value: &str) -> ThreadId {
    ThreadId::new(value).expect("test Thread id is valid")
}

fn profile(key: &str) -> ResidentProfile {
    ResidentProfile {
        key: resident_key(key),
        accepted_messages: BTreeSet::new(),
        provided_capabilities: BTreeSet::new(),
        state_machines: Vec::new(),
    }
}

fn capability_key(value: &str) -> CapabilityKey {
    CapabilityKey::new(value).expect("test capability key is valid")
}

fn response(value: Value) -> ResidentResponse {
    ResidentResponse::Ok(ResidentEffect {
        emissions: vec![Emission::return_to_caller(FlowMessage::new(
            message_kind("test.response"),
            value,
        ))],
        state_events: Vec::new(),
    })
}

fn encode_data(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:02x}")).collect()
}

fn packed_pointer(offset: usize, length: usize) -> u64 {
    ((offset as u64) << 32) | length as u64
}

fn static_wasm(resident_response: &ResidentResponse) -> Vec<u8> {
    let bytes = serde_json::to_vec(resident_response).unwrap();
    let packed = packed_pointer(0, bytes.len());
    wat::parse_str(format!(
        r#"(module
            (memory (export "memory") 1)
            (func (export "village_abi_version") (result i32) i32.const 1)
            (global $heap (mut i32) (i32.const 4096))
            (data (i32.const 0) "{}")
            (func (export "village_alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.tee $ptr
                local.get $len
                i32.add
                global.set $heap
                local.get $ptr)
            (func (export "village_receive") (param i32 i32) (result i64)
                i64.const {packed})
            (func (export "village_dealloc") (param i32 i32))
            (func (export "village_shutdown")))"#,
        encode_data(&bytes)
    ))
    .unwrap()
}

fn static_artifact(key: &str, value: Value) -> ResidentArtifact {
    ResidentArtifact::new(profile(key), static_wasm(&response(value)))
}

fn sequence_wasm(responses: &[ResidentResponse]) -> Vec<u8> {
    assert!(!responses.is_empty());
    let encoded: Vec<Vec<u8>> = responses
        .iter()
        .map(|response| serde_json::to_vec(response).unwrap())
        .collect();
    let offsets: Vec<usize> = (0..encoded.len()).map(|index| index * 2048).collect();
    let data = encoded
        .iter()
        .zip(&offsets)
        .map(|(bytes, offset)| format!("(data (i32.const {offset}) \"{}\")", encode_data(bytes)))
        .collect::<Vec<_>>()
        .join("\n");

    let mut selection = format!(
        "i64.const {}",
        packed_pointer(*offsets.last().unwrap(), encoded.last().unwrap().len())
    );
    for index in (0..encoded.len().saturating_sub(1)).rev() {
        selection = format!(
            "global.get $count\ni32.const {index}\ni32.eq\nif (result i64)\n  i64.const {}\nelse\n{selection}\nend",
            packed_pointer(offsets[index], encoded[index].len())
        );
    }

    wat::parse_str(format!(
        r#"(module
            (memory (export "memory") 1)
            (func (export "village_abi_version") (result i32) i32.const 1)
            (global $heap (mut i32) (i32.const 8192))
            (global $count (mut i32) (i32.const 0))
            {data}
            (func (export "village_alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.tee $ptr
                local.get $len
                i32.add
                global.set $heap
                local.get $ptr)
            (func (export "village_receive") (param i32 i32) (result i64)
                (local $result i64)
                {selection}
                local.set $result
                global.get $count
                i32.const 1
                i32.add
                global.set $count
                local.get $result)
            (func (export "village_dealloc") (param i32 i32))
            (func (export "village_shutdown")))"#
    ))
    .unwrap()
}

fn directory_probe_wasm(needle: &str) -> Vec<u8> {
    let success = serde_json::to_vec(&response(json!({ "directory_visible": true }))).unwrap();
    let failure = serde_json::to_vec(&response(json!({ "directory_visible": false }))).unwrap();
    let comparisons = needle
        .bytes()
        .enumerate()
        .map(|(offset, byte)| {
            format!(
                "local.get $ptr\nlocal.get $index\ni32.add\ni32.const {offset}\ni32.add\ni32.load8_u\ni32.const {byte}\ni32.eq"
            )
        })
        .reduce(|left, right| format!("{left}\n{right}\ni32.and"))
        .expect("directory probe needle is non-empty");

    wat::parse_str(format!(
        r#"(module
            (memory (export "memory") 1)
            (func (export "village_abi_version") (result i32) i32.const 1)
            (global $heap (mut i32) (i32.const 4096))
            (data (i32.const 0) "{}")
            (data (i32.const 2048) "{}")
            (func $contains (param $ptr i32) (param $len i32) (result i32)
                (local $index i32)
                (block $not_found
                    (loop $scan
                        local.get $index
                        i32.const {}
                        i32.add
                        local.get $len
                        i32.gt_u
                        br_if $not_found
                        {comparisons}
                        if
                            i32.const 1
                            return
                        end
                        local.get $index
                        i32.const 1
                        i32.add
                        local.set $index
                        br $scan))
                i32.const 0)
            (func (export "village_alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.tee $ptr
                local.get $len
                i32.add
                global.set $heap
                local.get $ptr)
            (func (export "village_receive") (param $ptr i32) (param $len i32) (result i64)
                local.get $ptr
                local.get $len
                call $contains
                if (result i64)
                    i64.const {}
                else
                    i64.const {}
                end)
            (func (export "village_dealloc") (param i32 i32)))"#,
        encode_data(&success),
        encode_data(&failure),
        needle.len(),
        packed_pointer(0, success.len()),
        packed_pointer(2048, failure.len()),
    ))
    .unwrap()
}

fn input() -> FlowMessage {
    FlowMessage::new(message_kind("test.request"), Value::Null)
}

struct TagGateFactory {
    tag: &'static str,
    fail_initialization: bool,
}

#[async_trait]
impl GateFactory for TagGateFactory {
    fn profile(&self) -> GateProfile {
        GateProfile {
            key: gate_key("tag"),
            order: 0,
            hooks: BTreeSet::from([GateHookBinding {
                resident: resident_key("entry"),
                point: ResidentHookPoint::AfterExecute,
            }]),
        }
    }

    async fn instantiate(&self, _context: GateInitContext) -> Result<Box<dyn Gate>, GateError> {
        if self.fail_initialization {
            return Err(GateError::new("TEST_GATE_INIT", "requested failure"));
        }
        Ok(Box::new(TagGate { tag: self.tag }))
    }
}

struct TagGate {
    tag: &'static str,
}

#[async_trait]
impl Gate for TagGate {
    async fn after_execute(
        &mut self,
        _context: GateContext,
        _packet: &FlowPacket,
        mut effect: ResidentEffect,
    ) -> Result<ResidentEffect, GateError> {
        for emission in &mut effect.emissions {
            emission.message.payload["gate"] = Value::String(self.tag.to_owned());
        }
        Ok(effect)
    }
}

struct LifecycleGateFactory {
    key: &'static str,
    hooks: BTreeSet<GateHookBinding>,
    events: Arc<StdMutex<Vec<&'static str>>>,
}

#[async_trait]
impl GateFactory for LifecycleGateFactory {
    fn profile(&self) -> GateProfile {
        GateProfile {
            key: gate_key(self.key),
            order: 0,
            hooks: self.hooks.clone(),
        }
    }

    async fn instantiate(&self, _context: GateInitContext) -> Result<Box<dyn Gate>, GateError> {
        Ok(Box::new(LifecycleGate {
            key: self.key,
            events: self.events.clone(),
        }))
    }
}

struct LifecycleGate {
    key: &'static str,
    events: Arc<StdMutex<Vec<&'static str>>>,
}

impl LifecycleGate {
    fn record(&self) {
        self.events.lock().unwrap().push(self.key);
    }
}

#[async_trait]
impl Gate for LifecycleGate {
    async fn before_receive(
        &mut self,
        _context: GateContext,
        packet: &FlowPacket,
    ) -> Result<BeforeReceiveAction, GateError> {
        self.record();
        Ok(BeforeReceiveAction::Continue(packet.message.clone()))
    }

    async fn after_execute(
        &mut self,
        _context: GateContext,
        _packet: &FlowPacket,
        effect: ResidentEffect,
    ) -> Result<ResidentEffect, GateError> {
        self.record();
        Ok(effect)
    }

    async fn before_commit(
        &mut self,
        _context: GateContext,
        _packet: &FlowPacket,
        effect: ResidentEffect,
    ) -> Result<ResidentEffect, GateError> {
        self.record();
        Ok(effect)
    }

    async fn on_failure(
        &mut self,
        _context: GateContext,
        _failure: RuntimeFailure,
    ) -> Result<FailureResolution, GateError> {
        self.record();
        Ok(FailureResolution::Propagate)
    }
}

fn gate_hook(resident: &str, point: ResidentHookPoint) -> GateHookBinding {
    GateHookBinding {
        resident: resident_key(resident),
        point,
    }
}

#[tokio::test]
async fn resident_instances_have_separate_wasm_stores_per_thread() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let wasm = sequence_wasm(&[
        response(json!({ "call": "first" })),
        response(json!({ "call": "second" })),
    ]);
    rdf.submit(
        RegistrationBatch::new().upsert_resident(ResidentArtifact::new(profile("entry"), wasm)),
    )
    .await
    .unwrap();

    let first_a = rtdf
        .run_turn(thread_id("thread-a"), resident_key("entry"), input())
        .await
        .unwrap();
    let second_a = rtdf
        .run_turn(thread_id("thread-a"), resident_key("entry"), input())
        .await
        .unwrap();
    let first_b = rtdf
        .run_turn(thread_id("thread-b"), resident_key("entry"), input())
        .await
        .unwrap();

    assert_eq!(first_a.returned[0].payload["call"], "first");
    assert_eq!(second_a.returned[0].payload["call"], "second");
    assert_eq!(first_b.returned[0].payload["call"], "first");
}

#[tokio::test]
async fn resident_lifecycle_runs_only_the_hooks_mounted_to_that_resident() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let events = Arc::new(StdMutex::new(Vec::new()));
    let gate = |key, resident, point| {
        Arc::new(LifecycleGateFactory {
            key,
            hooks: BTreeSet::from([gate_hook(resident, point)]),
            events: events.clone(),
        }) as Arc<dyn GateFactory>
    };
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(static_artifact("entry", json!({ "ok": true })))
            .upsert_resident(static_artifact("idle", json!({ "idle": true })))
            .upsert_gate(gate("01-before", "entry", ResidentHookPoint::BeforeReceive))
            .upsert_gate(gate("02-after", "entry", ResidentHookPoint::AfterExecute))
            .upsert_gate(gate("03-commit", "entry", ResidentHookPoint::BeforeCommit))
            .upsert_gate(gate(
                "idle-before",
                "idle",
                ResidentHookPoint::BeforeReceive,
            )),
    )
    .await
    .unwrap();

    rtdf.run_turn(thread_id("thread-a"), resident_key("entry"), input())
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec!["01-before", "02-after", "03-commit"]
    );
}

#[tokio::test]
async fn rdf_rejects_unbound_or_dangling_gate_hooks() {
    let rdf = RegistrationDataFlow::new();
    let events = Arc::new(StdMutex::new(Vec::new()));
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(static_artifact("entry", json!({ "ok": true })))
            .upsert_gate(Arc::new(LifecycleGateFactory {
                key: "unbound",
                hooks: BTreeSet::new(),
                events: events.clone(),
            })),
    )
    .await
    .unwrap();
    let unbound = rdf.discover().await;
    assert!(matches!(
        unbound.failures[0].error,
        RegistrationError::UnboundGate { .. }
    ));

    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(static_artifact("entry", json!({ "ok": true })))
            .upsert_gate(Arc::new(LifecycleGateFactory {
                key: "dangling",
                hooks: BTreeSet::from([gate_hook("missing", ResidentHookPoint::BeforeReceive)]),
                events,
            })),
    )
    .await
    .unwrap();
    let dangling = rdf.discover().await;
    assert!(matches!(
        dangling.failures[0].error,
        RegistrationError::UnknownGateResident { .. }
    ));
}

#[tokio::test]
async fn resident_receives_a_data_only_directory_snapshot() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let mut receiver_profile = profile("directory-only-resident");
    receiver_profile
        .provided_capabilities
        .insert(capability_key("memory.search"));
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(ResidentArtifact::new(
                profile("probe"),
                directory_probe_wasm("directory-only-resident"),
            ))
            .upsert_resident(ResidentArtifact::new(
                receiver_profile,
                static_wasm(&response(json!({ "unused": true }))),
            )),
    )
    .await
    .unwrap();

    let output = rtdf
        .run_turn(thread_id("thread-a"), resident_key("probe"), input())
        .await
        .unwrap();
    assert_eq!(output.returned[0].payload["directory_visible"], true);

    let directory = rdf.active_snapshot().await.resident_directory();
    assert_eq!(directory.snapshot_id, output.snapshot_id);
    let providers: Vec<_> = directory
        .providers(&capability_key("memory.search"))
        .map(|descriptor| descriptor.key.clone())
        .collect();
    assert_eq!(providers, vec![resident_key("directory-only-resident")]);
    let serialized = serde_json::to_value(&directory).unwrap();
    let descriptor = serialized["residents"]["directory-only-resident"]
        .as_object()
        .unwrap();
    assert_eq!(
        descriptor
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["accepted_messages", "key", "provided_capabilities"])
    );
}

#[tokio::test]
async fn resident_with_any_host_import_is_rejected_before_discovery() {
    let rdf = RegistrationDataFlow::new();
    let wasm =
        wat::parse_str(r#"(module (import "wasi_snapshot_preview1" "fd_write" (func $write)))"#)
            .unwrap();
    let error = rdf
        .submit(
            RegistrationBatch::new().upsert_resident(ResidentArtifact::new(profile("entry"), wasm)),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RegistrationError::ResidentImportForbidden { .. }
    ));
}

#[tokio::test]
async fn target_receive_hook_runs_when_dynamic_emission_arrives() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let events = Arc::new(StdMutex::new(Vec::new()));
    let relay = ResidentResponse::Ok(ResidentEffect {
        emissions: vec![Emission::to(
            resident_key("receiver"),
            FlowMessage::new(message_kind("test.relay"), Value::Null),
        )],
        state_events: Vec::new(),
    });
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(ResidentArtifact::new(
                profile("sender"),
                static_wasm(&relay),
            ))
            .upsert_resident(static_artifact("receiver", json!({ "received": true })))
            .upsert_gate(Arc::new(LifecycleGateFactory {
                key: "receiver-before",
                hooks: BTreeSet::from([gate_hook("receiver", ResidentHookPoint::BeforeReceive)]),
                events: events.clone(),
            })),
    )
    .await
    .unwrap();

    let output = rtdf
        .run_turn(thread_id("thread-a"), resident_key("sender"), input())
        .await
        .unwrap();
    assert_eq!(output.returned[0].payload["received"], true);
    assert_eq!(*events.lock().unwrap(), vec!["receiver-before"]);
}

#[tokio::test]
async fn related_resident_and_gate_update_rolls_back_as_one_unit() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(static_artifact("entry", json!({ "resident": "old" })))
            .upsert_gate(Arc::new(TagGateFactory {
                tag: "old",
                fail_initialization: false,
            })),
    )
    .await
    .unwrap();

    let initial = rtdf
        .run_turn(thread_id("thread-a"), resident_key("entry"), input())
        .await
        .unwrap();
    assert_eq!(
        initial.returned[0].payload,
        json!({ "resident": "old", "gate": "old" })
    );

    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(static_artifact("entry", json!({ "resident": "new" })))
            .upsert_gate(Arc::new(TagGateFactory {
                tag: "new",
                fail_initialization: true,
            })),
    )
    .await
    .unwrap();

    let after_failed_update = rtdf
        .run_turn(thread_id("thread-a"), resident_key("entry"), input())
        .await
        .unwrap();
    assert_eq!(
        after_failed_update.returned[0].payload,
        json!({ "resident": "old", "gate": "old" })
    );
}

#[tokio::test]
async fn resident_routes_in_a_cycle_while_layout_commits_owned_state() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let machine = MachineKey::new("loop.machine").unwrap();
    let advance = EventId::new("advance").unwrap();
    let start = StateId::new("start").unwrap();
    let middle = StateId::new("middle").unwrap();
    let done = StateId::new("done").unwrap();
    let loop_effect = || {
        ResidentResponse::Ok(ResidentEffect {
            emissions: vec![Emission::to(resident_key("loop"), input())],
            state_events: vec![StateEvent {
                machine: machine.clone(),
                event: advance.clone(),
            }],
        })
    };
    let wasm = sequence_wasm(&[
        loop_effect(),
        loop_effect(),
        response(json!({ "state": "done" })),
    ]);
    let mut resident_profile = profile("loop");
    resident_profile
        .state_machines
        .push(StateMachineDefinition {
            key: machine.clone(),
            owner: resident_key("loop"),
            initial_state: start.clone(),
            states: BTreeSet::from([start.clone(), middle.clone(), done.clone()]),
            transitions: vec![
                TransitionRule {
                    from: start,
                    event: advance.clone(),
                    to: middle,
                },
                TransitionRule {
                    from: StateId::new("middle").unwrap(),
                    event: advance,
                    to: done.clone(),
                },
            ],
        });
    rdf.submit(
        RegistrationBatch::new().upsert_resident(ResidentArtifact::new(resident_profile, wasm)),
    )
    .await
    .unwrap();

    let output = rtdf
        .run_turn(thread_id("thread-a"), resident_key("loop"), input())
        .await
        .unwrap();
    assert_eq!(output.processed_packets, 3);
    assert_eq!(output.returned[0].payload["state"], "done");
    let context = rtdf.thread_snapshot(&thread_id("thread-a")).await.unwrap();
    assert_eq!(context.machine_states.get(&machine), Some(&done));
}

#[tokio::test]
async fn hard_sleep_drops_the_store_and_reconnects_with_fresh_resident_state() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let wasm = sequence_wasm(&[
        response(json!({ "call": "first" })),
        response(json!({ "call": "second" })),
    ]);
    rdf.submit(
        RegistrationBatch::new().upsert_resident(ResidentArtifact::new(profile("entry"), wasm)),
    )
    .await
    .unwrap();
    let id = thread_id("thread-a");

    let first = rtdf
        .run_turn(id.clone(), resident_key("entry"), input())
        .await
        .unwrap();
    assert_eq!(first.returned[0].payload["call"], "first");
    assert!(rtdf.sleep_thread(&id).await);
    assert!(rtdf.thread_snapshot(&id).await.is_none());

    let after_wake = rtdf
        .run_turn(id, resident_key("entry"), input())
        .await
        .unwrap();
    assert_eq!(after_wake.returned[0].payload["call"], "first");
}

#[tokio::test]
async fn fuel_exhaustion_is_reported_as_a_resident_failure() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let failure_events = Arc::new(StdMutex::new(Vec::new()));
    let wasm = wat::parse_str(
        r#"(module
            (memory (export "memory") 1)
            (func (export "village_abi_version") (result i32) i32.const 1)
            (func (export "village_alloc") (param i32) (result i32) i32.const 0)
            (func (export "village_receive") (param i32 i32) (result i64)
                (loop $forever br $forever)
                i64.const 0)
            (func (export "village_dealloc") (param i32 i32)))"#,
    )
    .unwrap();
    let limits = ResidentLimits {
        fuel_per_delivery: 1_000,
        ..ResidentLimits::default()
    };
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(ResidentArtifact::new(profile("entry"), wasm).with_limits(limits))
            .upsert_gate(Arc::new(LifecycleGateFactory {
                key: "failure",
                hooks: BTreeSet::from([gate_hook("entry", ResidentHookPoint::OnFailure)]),
                events: failure_events.clone(),
            })),
    )
    .await
    .unwrap();

    let error = rtdf
        .run_turn(thread_id("thread-a"), resident_key("entry"), input())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::ResidentFailure { ref code, .. } if code == "RESIDENT_TRAP"
    ));
    assert_eq!(*failure_events.lock().unwrap(), vec!["failure"]);
}

#[tokio::test]
async fn memory_limit_is_enforced_when_a_resident_store_is_created() {
    let rdf = Arc::new(RegistrationDataFlow::new());
    let rtdf = RuntimeDataFlow::new(rdf.clone());
    let wasm = wat::parse_str(
        r#"(module
            (memory (export "memory") 2)
            (func (export "village_abi_version") (result i32) i32.const 1)
            (func (export "village_alloc") (param i32) (result i32) i32.const 0)
            (func (export "village_receive") (param i32 i32) (result i64) i64.const 0)
            (func (export "village_dealloc") (param i32 i32)))"#,
    )
    .unwrap();
    let limits = ResidentLimits {
        max_memory_bytes: 64 * 1024,
        ..ResidentLimits::default()
    };
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(ResidentArtifact::new(profile("entry"), wasm).with_limits(limits)),
    )
    .await
    .unwrap();

    let error = rtdf
        .run_turn(thread_id("thread-a"), resident_key("entry"), input())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::Activation(ActivationError::ResidentInitialization { .. })
    ));
}

#[tokio::test]
async fn invalid_registration_batch_does_not_replace_the_active_snapshot() {
    let rdf = RegistrationDataFlow::new();
    let machine = MachineKey::new("shared.machine").unwrap();
    let state = StateId::new("ready").unwrap();
    let definition = |owner: &str| StateMachineDefinition {
        key: machine.clone(),
        owner: resident_key(owner),
        initial_state: state.clone(),
        states: BTreeSet::from([state.clone()]),
        transitions: Vec::new(),
    };
    let mut first = profile("one");
    first.state_machines.push(definition("one"));
    let mut second = profile("two");
    second.state_machines.push(definition("two"));
    rdf.submit(
        RegistrationBatch::new()
            .upsert_resident(ResidentArtifact::new(
                first,
                static_wasm(&response(json!({ "resident": "one" }))),
            ))
            .upsert_resident(ResidentArtifact::new(
                second,
                static_wasm(&response(json!({ "resident": "two" }))),
            )),
    )
    .await
    .unwrap();

    let report = rdf.discover().await;
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.snapshot.resident_count(), 0);
}
