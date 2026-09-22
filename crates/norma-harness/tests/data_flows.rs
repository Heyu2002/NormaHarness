use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use norma_harness::{
    CapabilityKey, FlowMessage, Gate, GateContext, GateError, Mailbox, MailboxAddress,
    MailboxError, MessageKind, MessageSender, NormaHarness, RegistrationError,
    RegistrationNoticeError, RegistrationSender, Resident, ResidentDescriptor, ResidentEvent,
    ResidentKey, RouteError,
};
use serde_json::json;

#[derive(Default)]
struct RecordingMailbox {
    events: Mutex<Vec<ResidentEvent>>,
    delivery_order: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl RecordingMailbox {
    fn tracking(delivery_order: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            delivery_order: Some(delivery_order),
        }
    }

    fn events(&self) -> Vec<ResidentEvent> {
        self.events.lock().expect("events lock poisoned").clone()
    }
}

impl Mailbox for RecordingMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        if let Some(order) = &self.delivery_order {
            order
                .lock()
                .expect("delivery order lock poisoned")
                .push("target-mailbox");
        }
        self.events
            .lock()
            .expect("events lock poisoned")
            .push(event);
        Ok(())
    }
}

struct RejectingMailbox;

impl Mailbox for RejectingMailbox {
    fn deliver(&self, _event: ResidentEvent) -> Result<(), MailboxError> {
        Err(MailboxError::new("mailbox is closed"))
    }
}

struct TestResident {
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
    outbound_gate: Option<Arc<dyn Gate>>,
    inbound_gate: Option<Arc<dyn Gate>>,
}

impl TestResident {
    fn new(
        key: ResidentKey,
        capabilities: impl IntoIterator<Item = CapabilityKey>,
        mailbox: MailboxAddress,
    ) -> Self {
        Self {
            descriptor: ResidentDescriptor::new(key, capabilities),
            mailbox,
            outbound_gate: None,
            inbound_gate: None,
        }
    }

    fn with_outbound_gate(mut self, gate: Arc<dyn Gate>) -> Self {
        self.outbound_gate = Some(gate);
        self
    }

    fn with_inbound_gate(mut self, gate: Arc<dyn Gate>) -> Self {
        self.inbound_gate = Some(gate);
        self
    }
}

impl Resident for TestResident {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }

    fn outbound_gate(&self) -> Option<Arc<dyn Gate>> {
        self.outbound_gate.clone()
    }

    fn inbound_gate(&self) -> Option<Arc<dyn Gate>> {
        self.inbound_gate.clone()
    }
}

struct WiredResident {
    resident: TestResident,
    _registration: RegistrationSender,
    _messages: MessageSender,
}

impl WiredResident {
    fn new(key: ResidentKey, registration: RegistrationSender, messages: MessageSender) -> Self {
        Self {
            resident: TestResident::new(key, [], Arc::new(RecordingMailbox::default())),
            _registration: registration,
            _messages: messages,
        }
    }
}

impl Resident for WiredResident {
    fn descriptor(&self) -> ResidentDescriptor {
        self.resident.descriptor()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.resident.mailbox()
    }
}

struct AppendGate {
    label: &'static str,
    order: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Gate for AppendGate {
    async fn pass(
        &self,
        _context: &GateContext,
        message: FlowMessage,
    ) -> Result<FlowMessage, GateError> {
        self.order
            .lock()
            .expect("gate order lock poisoned")
            .push(self.label);
        let (kind, payload) = message.into_parts();
        let text = payload
            .as_str()
            .expect("test message payload should be a string");
        Ok(FlowMessage::new(
            kind,
            json!(format!("{text}>{}", self.label)),
        ))
    }
}

struct RejectGate;

#[async_trait]
impl Gate for RejectGate {
    async fn pass(
        &self,
        _context: &GateContext,
        _message: FlowMessage,
    ) -> Result<FlowMessage, GateError> {
        Err(GateError::new("blocked"))
    }
}

fn resident(value: &str) -> ResidentKey {
    ResidentKey::new(value).expect("valid Resident name")
}

fn capability(value: &str) -> CapabilityKey {
    CapabilityKey::new(value).expect("valid capability name")
}

fn message_kind(value: &str) -> MessageKind {
    MessageKind::new(value).expect("valid message kind")
}

#[tokio::test]
async fn resident_initiated_registration_injects_instance_and_announces_it() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let first_mailbox = Arc::new(RecordingMailbox::default());
    let first = Arc::new(TestResident::new(
        resident("first"),
        [capability("summarize")],
        first_mailbox.clone(),
    ));
    registration
        .register(first)
        .await
        .expect("first Resident should register itself");

    let second = Arc::new(TestResident::new(
        resident("second"),
        [capability("store")],
        Arc::new(RecordingMailbox::default()),
    ));
    let receipt = registration
        .register(second)
        .await
        .expect("second Resident should register itself");

    assert_eq!(receipt.existing_residents().len(), 1);
    assert_eq!(receipt.existing_residents()[0].key(), &resident("first"));
    assert!(receipt.notice_failures().is_empty());
    assert_eq!(application.rdf().stored_instance_count().await, 2);
    assert_eq!(
        first_mailbox.events(),
        vec![ResidentEvent::ResidentRegistered(ResidentDescriptor::new(
            resident("second"),
            [capability("store")]
        ))]
    );
}

#[tokio::test]
async fn rdf_remains_the_only_same_name_authority() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let first = registration
        .register(Arc::new(TestResident::new(
            resident("same-name"),
            [],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect("first Resident should register");

    let error = registration
        .register(Arc::new(TestResident::new(
            resident("same-name"),
            [],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect_err("RDF must reject a duplicate Resident name");

    assert!(matches!(
        error,
        RegistrationError::DuplicateResident {
            registered_instance,
            ..
        } if registered_instance == first.instance_id()
    ));
    assert_eq!(application.rdf().resident_count().await, 1);
    assert_eq!(application.rdf().stored_instance_count().await, 1);
}

#[tokio::test]
async fn rdf_reads_capabilities_from_the_injected_instance() {
    let application = NormaHarness::new();
    application
        .registration_sender()
        .register(Arc::new(TestResident::new(
            resident("search-service"),
            [capability("search")],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect("Resident should register");

    let providers = application.rdf().providers(&capability("search")).await;
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].key(), &resident("search-service"));
}

#[tokio::test]
async fn rtdf_uses_endpoints_captured_by_rdf_in_gate_order() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let messages = application.message_sender();
    let order = Arc::new(Mutex::new(Vec::new()));

    let source = Arc::new(
        TestResident::new(
            resident("source"),
            [],
            Arc::new(RecordingMailbox::default()),
        )
        .with_outbound_gate(Arc::new(AppendGate {
            label: "source-outbound",
            order: order.clone(),
        })),
    );
    let source_id = registration
        .register(source)
        .await
        .expect("source should register")
        .instance_id();

    let target_mailbox = Arc::new(RecordingMailbox::tracking(order.clone()));
    let target = Arc::new(
        TestResident::new(resident("target"), [], target_mailbox.clone()).with_inbound_gate(
            Arc::new(AppendGate {
                label: "target-inbound",
                order: order.clone(),
            }),
        ),
    );
    registration
        .register(target)
        .await
        .expect("target should register");

    messages
        .send(
            source_id,
            &resident("target"),
            FlowMessage::new(message_kind("request"), json!("start")),
        )
        .await
        .expect("message should be delivered");

    assert_eq!(
        *order.lock().expect("order lock poisoned"),
        vec!["source-outbound", "target-inbound", "target-mailbox"]
    );
    let events = target_mailbox.events();
    let ResidentEvent::Message(message) = &events[0] else {
        panic!("target should receive a routed message");
    };
    assert_eq!(
        message.message().payload(),
        &json!("start>source-outbound>target-inbound")
    );
}

#[tokio::test]
async fn gate_rejection_stops_delivery() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let source_id = registration
        .register(Arc::new(
            TestResident::new(
                resident("source"),
                [],
                Arc::new(RecordingMailbox::default()),
            )
            .with_outbound_gate(Arc::new(RejectGate)),
        ))
        .await
        .expect("source should register")
        .instance_id();
    let target_mailbox = Arc::new(RecordingMailbox::default());
    registration
        .register(Arc::new(TestResident::new(
            resident("target"),
            [],
            target_mailbox.clone(),
        )))
        .await
        .expect("target should register");

    let error = application
        .message_sender()
        .send(
            source_id,
            &resident("target"),
            FlowMessage::new(message_kind("request"), json!(null)),
        )
        .await
        .expect_err("outbound Gate should reject the message");

    assert!(matches!(error, RouteError::OutboundGate { .. }));
    assert!(target_mailbox.events().is_empty());
}

#[tokio::test]
async fn old_instance_id_cannot_send_after_same_name_is_reused() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let messages = application.message_sender();
    registration
        .register(Arc::new(TestResident::new(
            resident("target"),
            [],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect("target should register");

    let old_id = registration
        .register(Arc::new(TestResident::new(
            resident("source"),
            [],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect("old source should register")
        .instance_id();
    registration
        .unregister(old_id)
        .await
        .expect("old source should unregister last");

    let new_id = registration
        .register(Arc::new(TestResident::new(
            resident("source"),
            [],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect("new source should reuse the name")
        .instance_id();
    assert_ne!(old_id, new_id);

    let error = messages
        .send(
            old_id,
            &resident("target"),
            FlowMessage::new(message_kind("request"), json!(null)),
        )
        .await
        .expect_err("an unregistered instance must not send");
    assert_eq!(
        error,
        RouteError::UnknownSource {
            instance_id: old_id
        }
    );
}

#[tokio::test]
async fn mailbox_failure_does_not_unregister_the_target() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let source_id = registration
        .register(Arc::new(TestResident::new(
            resident("source"),
            [],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect("source should register")
        .instance_id();
    let target_id = registration
        .register(Arc::new(TestResident::new(
            resident("target"),
            [],
            Arc::new(RejectingMailbox),
        )))
        .await
        .expect("target should register")
        .instance_id();

    let error = application
        .message_sender()
        .send(
            source_id,
            &resident("target"),
            FlowMessage::new(message_kind("request"), json!(null)),
        )
        .await
        .expect_err("closed mailbox should reject delivery");
    assert!(matches!(error, RouteError::TargetMailbox { .. }));
    assert!(application.rdf().is_registered(target_id).await);
}

#[tokio::test]
async fn failed_registration_notice_is_reported_without_removal() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let closed_id = registration
        .register(Arc::new(TestResident::new(
            resident("closed"),
            [],
            Arc::new(RejectingMailbox),
        )))
        .await
        .expect("closed Resident should register")
        .instance_id();

    let receipt = registration
        .register(Arc::new(TestResident::new(
            resident("live"),
            [],
            Arc::new(RecordingMailbox::default()),
        )))
        .await
        .expect("notice failure must not reject registration");

    assert_eq!(receipt.notice_failures().len(), 1);
    assert!(matches!(
        receipt.notice_failures()[0].error(),
        RegistrationNoticeError::Mailbox(_)
    ));
    assert!(application.rdf().is_registered(closed_id).await);
    assert_eq!(application.rdf().resident_count().await, 2);
}

#[tokio::test]
async fn unregister_releases_rdfs_strong_instance_reference() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let resident = Arc::new(WiredResident::new(
        resident("wired"),
        registration.clone(),
        application.message_sender(),
    ));
    let weak = Arc::downgrade(&resident);
    let instance_id = registration
        .register(resident.clone())
        .await
        .expect("wired Resident should register")
        .instance_id();
    drop(resident);

    assert!(
        weak.upgrade().is_some(),
        "RDF Store should own the instance"
    );
    registration
        .unregister(instance_id)
        .await
        .expect("unregistration should release Store ownership");
    assert!(weak.upgrade().is_none(), "channel ports must not own RDF");
    assert_eq!(application.rdf().stored_instance_count().await, 0);
}

#[tokio::test]
async fn dropping_harness_breaks_no_hidden_reference_cycle() {
    let application = NormaHarness::new();
    let registration = application.registration_sender();
    let messages = application.message_sender();
    let wired = Arc::new(WiredResident::new(
        resident("wired"),
        registration.clone(),
        messages.clone(),
    ));
    let weak = Arc::downgrade(&wired);
    let instance_id = registration
        .register(wired.clone())
        .await
        .expect("wired Resident should register")
        .instance_id();
    drop(wired);
    drop(application);

    assert!(
        weak.upgrade().is_none(),
        "RDF, RTDF, Store, Resident, and injected senders must form no strong cycle"
    );
    assert_eq!(
        registration
            .register(Arc::new(TestResident::new(
                resident("after-drop"),
                [],
                Arc::new(RecordingMailbox::default()),
            )))
            .await,
        Err(RegistrationError::PipelineUnavailable)
    );
    assert_eq!(
        messages
            .send(
                instance_id,
                &resident("wired"),
                FlowMessage::new(message_kind("after-drop"), json!(null)),
            )
            .await,
        Err(RouteError::PipelineUnavailable)
    );
}
