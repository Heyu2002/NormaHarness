use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};
use village_harness_protocol::{
    BeforeReceiveAction, ExposureId, FailureResolution, FlowMessage, FlowPacket, FlowTarget, Gate,
    GateContext, GateInitContext, GateKey, GateProfile, LayoutAccess, ResidentContextSnapshot,
    ResidentDirectorySnapshot, ResidentEffect, ResidentFailureStage, ResidentHookPoint,
    ResidentInvocation, ResidentKey, ResidentProfile, RuntimeFailure, StateEvent, ThreadId,
    TurnLimits, TurnOutput,
};

use crate::{
    ActivationError, AgentThreadSnapshot, RegistrationDataFlow, RegistrationSnapshot, RuntimeError,
    context::{AgentThreadContext, ThreadLayoutAccess},
    resident_host::ResidentMessenger,
};

type GateInstance = Arc<Mutex<Box<dyn Gate>>>;

#[derive(Clone)]
struct ResidentBinding {
    exposure_id: ExposureId,
    profile: ResidentProfile,
    messenger: ResidentMessenger,
}

#[derive(Clone)]
struct GateBinding {
    exposure_id: ExposureId,
    profile: GateProfile,
    instance: GateInstance,
}

#[derive(Clone, Default)]
struct ResidentHookRegistry {
    chains: BTreeMap<(ResidentKey, ResidentHookPoint), Vec<GateBinding>>,
}

impl ResidentHookRegistry {
    fn from_gates(gates: &[GateBinding]) -> Self {
        let mut chains = BTreeMap::new();
        for gate in gates {
            for hook in &gate.profile.hooks {
                chains
                    .entry((hook.resident.clone(), hook.point))
                    .or_insert_with(Vec::new)
                    .push(gate.clone());
            }
        }
        Self { chains }
    }

    fn chain(&self, resident: &ResidentKey, point: ResidentHookPoint) -> &[GateBinding] {
        self.chains
            .get(&(resident.clone(), point))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

#[derive(Clone)]
struct RuntimeSet {
    snapshot: Arc<RegistrationSnapshot>,
    directory: ResidentDirectorySnapshot,
    residents: BTreeMap<ResidentKey, ResidentBinding>,
    gates: Vec<GateBinding>,
    hooks: ResidentHookRegistry,
}

struct ThreadState {
    closed: bool,
    runtime: Option<RuntimeSet>,
    last_active: Instant,
}

struct ThreadRuntime {
    context: Arc<RwLock<AgentThreadContext>>,
    state: Mutex<ThreadState>,
}

impl ThreadRuntime {
    fn new(thread_id: ThreadId) -> Self {
        Self {
            context: Arc::new(RwLock::new(AgentThreadContext::new(thread_id))),
            state: Mutex::new(ThreadState {
                closed: false,
                runtime: None,
                last_active: Instant::now(),
            }),
        }
    }
}

pub struct RuntimeDataFlow {
    rdf: Arc<RegistrationDataFlow>,
    threads: RwLock<HashMap<ThreadId, Arc<ThreadRuntime>>>,
    limits: TurnLimits,
}

impl std::fmt::Debug for RuntimeDataFlow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeDataFlow")
            .field("rdf", &self.rdf)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl RuntimeDataFlow {
    pub fn new(rdf: Arc<RegistrationDataFlow>) -> Self {
        Self::with_limits(rdf, TurnLimits::default())
    }

    pub fn with_limits(rdf: Arc<RegistrationDataFlow>, limits: TurnLimits) -> Self {
        Self {
            rdf,
            threads: RwLock::new(HashMap::new()),
            limits,
        }
    }

    pub async fn run_turn(
        &self,
        thread_id: ThreadId,
        entry_resident: ResidentKey,
        message: FlowMessage,
    ) -> Result<TurnOutput, RuntimeError> {
        loop {
            let thread = self.get_or_create_thread(thread_id.clone()).await;
            let mut state = thread.state.lock().await;
            if state.closed {
                drop(state);
                continue;
            }

            let discovery = self.rdf.discover().await;
            for failure in discovery.failures {
                warn!(batch_id = %failure.batch_id, error = %failure.error, "RDF rejected a registration batch");
            }

            if state.runtime.as_ref().map(|runtime| runtime.snapshot.id())
                != Some(discovery.snapshot.id())
            {
                match self
                    .activate(
                        &thread_id,
                        &thread,
                        state.runtime.as_ref(),
                        discovery.snapshot,
                    )
                    .await
                {
                    Ok(runtime) => {
                        let activated = runtime.clone();
                        let previous = state.runtime.replace(runtime);
                        if let Some(previous) = previous {
                            shutdown_replaced(&previous, &activated).await;
                        }
                    }
                    Err(error) if state.runtime.is_some() => {
                        warn!(thread_id = %thread_id, error = %error, "hot update rolled back for AgentThread");
                    }
                    Err(error) => return Err(RuntimeError::Activation(error)),
                }
            }

            let runtime = state
                .runtime
                .as_ref()
                .expect("successful activation always installs a RuntimeSet")
                .clone();
            let layout: Arc<dyn LayoutAccess> = Arc::new(ThreadLayoutAccess::new(
                thread_id.clone(),
                thread.context.clone(),
            ));
            let result = self
                .execute_turn(runtime, layout, &thread.context, entry_resident, message)
                .await;
            state.last_active = Instant::now();
            return result;
        }
    }

    pub async fn thread_snapshot(&self, thread_id: &ThreadId) -> Option<AgentThreadSnapshot> {
        let thread = self.threads.read().await.get(thread_id).cloned()?;
        let snapshot = thread.context.read().await.snapshot();
        Some(snapshot)
    }

    pub async fn sleep_thread(&self, thread_id: &ThreadId) -> bool {
        let Some(thread) = self.threads.read().await.get(thread_id).cloned() else {
            return false;
        };
        self.close_thread(thread_id, thread, None).await
    }

    pub async fn sleep_idle(&self, idle_for: Duration) -> usize {
        let candidates: Vec<(ThreadId, Arc<ThreadRuntime>)> = self
            .threads
            .read()
            .await
            .iter()
            .map(|(id, thread)| (id.clone(), thread.clone()))
            .collect();
        let mut slept = 0;
        for (thread_id, thread) in candidates {
            if self.close_thread(&thread_id, thread, Some(idle_for)).await {
                slept += 1;
            }
        }
        slept
    }

    pub async fn active_thread_count(&self) -> usize {
        self.threads.read().await.len()
    }

    async fn get_or_create_thread(&self, thread_id: ThreadId) -> Arc<ThreadRuntime> {
        if let Some(thread) = self.threads.read().await.get(&thread_id).cloned() {
            return thread;
        }
        let mut threads = self.threads.write().await;
        threads
            .entry(thread_id.clone())
            .or_insert_with(|| Arc::new(ThreadRuntime::new(thread_id)))
            .clone()
    }

    async fn close_thread(
        &self,
        thread_id: &ThreadId,
        thread: Arc<ThreadRuntime>,
        minimum_idle: Option<Duration>,
    ) -> bool {
        let mut state = thread.state.lock().await;
        if state.closed || minimum_idle.is_some_and(|idle| state.last_active.elapsed() < idle) {
            return false;
        }
        state.closed = true;

        {
            let mut threads = self.threads.write().await;
            if threads
                .get(thread_id)
                .is_some_and(|current| Arc::ptr_eq(current, &thread))
            {
                threads.remove(thread_id);
            }
        }

        if let Some(runtime) = state.runtime.take() {
            shutdown_runtime(runtime).await;
        }
        debug!(thread_id = %thread_id, "AgentThread entered hard sleep");
        true
    }

    async fn activate(
        &self,
        thread_id: &ThreadId,
        thread: &ThreadRuntime,
        current: Option<&RuntimeSet>,
        snapshot: Arc<RegistrationSnapshot>,
    ) -> Result<RuntimeSet, ActivationError> {
        let definitions = snapshot.machine_definitions();
        let staged_states = thread.context.read().await.stage_states(&definitions)?;
        let layout: Arc<dyn LayoutAccess> = Arc::new(ThreadLayoutAccess::new(
            thread_id.clone(),
            thread.context.clone(),
        ));
        let gate_init_context = GateInitContext {
            layout: layout.clone(),
        };

        let mut residents = BTreeMap::new();
        let mut created_residents = Vec::new();
        for (key, registered) in &snapshot.residents {
            if let Some(binding) = reusable_resident(current, key, registered.exposure_id) {
                residents.insert(key.clone(), binding);
                continue;
            }
            match registered.driver.instantiate(key.clone()) {
                Ok(messenger) => {
                    created_residents.push(messenger.clone());
                    residents.insert(
                        key.clone(),
                        ResidentBinding {
                            exposure_id: registered.exposure_id,
                            profile: registered.profile.clone(),
                            messenger,
                        },
                    );
                }
                Err(error) => {
                    shutdown_created_residents(created_residents).await;
                    return Err(ActivationError::ResidentInitialization {
                        resident: key.clone(),
                        code: error.code().to_owned(),
                        message: error.message().to_owned(),
                    });
                }
            }
        }

        let mut gates = Vec::new();
        let mut created_gates = Vec::new();
        for registered in snapshot.gates.values() {
            if let Some(binding) =
                reusable_gate(current, &registered.profile.key, registered.exposure_id)
            {
                gates.push(binding);
                continue;
            }
            match registered
                .factory
                .instantiate(gate_init_context.clone())
                .await
            {
                Ok(instance) => {
                    let instance = Arc::new(Mutex::new(instance));
                    created_gates.push(instance.clone());
                    gates.push(GateBinding {
                        exposure_id: registered.exposure_id,
                        profile: registered.profile.clone(),
                        instance,
                    });
                }
                Err(error) => {
                    shutdown_created_residents(created_residents).await;
                    shutdown_created_gates(created_gates).await;
                    return Err(ActivationError::GateInitialization {
                        gate: registered.profile.key.clone(),
                        code: error.code,
                        message: error.message,
                    });
                }
            }
        }
        gates.sort_by(|left, right| {
            left.profile
                .order
                .cmp(&right.profile.order)
                .then_with(|| left.profile.key.cmp(&right.profile.key))
        });
        let hooks = ResidentHookRegistry::from_gates(&gates);

        thread
            .context
            .write()
            .await
            .activate(snapshot.id(), staged_states);

        let directory = snapshot.resident_directory();
        Ok(RuntimeSet {
            snapshot,
            directory,
            residents,
            gates,
            hooks,
        })
    }

    async fn execute_turn(
        &self,
        runtime: RuntimeSet,
        layout: Arc<dyn LayoutAccess>,
        context: &Arc<RwLock<AgentThreadContext>>,
        entry_resident: ResidentKey,
        message: FlowMessage,
    ) -> Result<TurnOutput, RuntimeError> {
        let mut queue = VecDeque::from([FlowPacket::ingress(entry_resident, message)]);
        let mut returned = Vec::new();
        let mut processed_packets = 0usize;

        while let Some(mut packet) = queue.pop_front() {
            if processed_packets >= self.limits.max_processed_packets {
                return Err(RuntimeError::PacketLimitExceeded {
                    limit: self.limits.max_processed_packets,
                });
            }
            if packet.hop > self.limits.max_hops_per_packet {
                return Err(RuntimeError::HopLimitExceeded {
                    limit: self.limits.max_hops_per_packet,
                });
            }

            let FlowTarget::Resident(target) = &packet.target else {
                returned.push(packet.message);
                continue;
            };
            let target = target.clone();
            let Some(binding) = runtime.residents.get(&target) else {
                return Err(RuntimeError::UnknownTarget(target));
            };

            match run_before_receive_hooks(&runtime, layout.clone(), &target, packet).await? {
                BeforeReceiveOutcome::Continue(next) => packet = next,
                BeforeReceiveOutcome::Drop => continue,
                BeforeReceiveOutcome::Redirect(redirect) => {
                    queue.push_front(redirect);
                    continue;
                }
            };
            if !binding.profile.accepts(&packet.message.kind) {
                let kind = packet.message.kind.clone();
                let failure = RuntimeFailure {
                    resident: target.clone(),
                    stage: ResidentFailureStage::BeforeReceive,
                    code: "UNSUPPORTED_MESSAGE".into(),
                    message: format!("Resident {target} does not accept message kind {kind}"),
                };
                match resolve_resident_failure(&runtime, layout.clone(), &target, failure).await? {
                    FailureOutcome::Redirect(redirect) => queue.push_back(redirect),
                    FailureOutcome::Drop => {}
                    FailureOutcome::Unhandled => {
                        return Err(RuntimeError::UnsupportedMessage {
                            resident: target,
                            kind,
                        });
                    }
                }
                continue;
            }

            processed_packets += 1;
            let invocation = {
                let context = context.read().await;
                ResidentInvocation {
                    packet: packet.clone(),
                    context: ResidentContextSnapshot {
                        thread_id: context.thread_id.clone(),
                        active_snapshot: context
                            .active_snapshot
                            .expect("Resident delivery only occurs after a snapshot is activated"),
                        machine_states: context.machine_states.clone(),
                        directory: runtime.directory.clone(),
                    },
                }
            };
            let outcome = binding.messenger.deliver(invocation).await;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    let failure = RuntimeFailure {
                        resident: target.clone(),
                        stage: ResidentFailureStage::Execute,
                        code: error.code.clone(),
                        message: error.message.clone(),
                    };
                    match resolve_resident_failure(&runtime, layout.clone(), &target, failure)
                        .await?
                    {
                        FailureOutcome::Redirect(redirect) => {
                            queue.push_back(redirect);
                            continue;
                        }
                        FailureOutcome::Drop => continue,
                        FailureOutcome::Unhandled => {
                            return Err(RuntimeError::ResidentFailure {
                                resident: target,
                                code: error.code,
                                message: error.message,
                            });
                        }
                    }
                }
            };

            let outcome = run_effect_hooks(
                &runtime,
                layout.clone(),
                &target,
                ResidentHookPoint::AfterExecute,
                &packet,
                outcome,
            )
            .await?;
            let outcome = run_effect_hooks(
                &runtime,
                layout.clone(),
                &target,
                ResidentHookPoint::BeforeCommit,
                &packet,
                outcome,
            )
            .await?;

            let commit_result =
                commit_transitions(context, &runtime.snapshot, &target, outcome.state_events).await;
            if let Err(error) = commit_result {
                let failure = RuntimeFailure {
                    resident: target.clone(),
                    stage: ResidentFailureStage::Commit,
                    code: "STATE_COMMIT".into(),
                    message: error.to_string(),
                };
                match resolve_resident_failure(&runtime, layout.clone(), &target, failure).await? {
                    FailureOutcome::Redirect(redirect) => queue.push_back(redirect),
                    FailureOutcome::Drop => {}
                    FailureOutcome::Unhandled => return Err(error),
                }
                continue;
            }

            for emission in outcome.emissions {
                let emitted = packet.emitted(emission);
                match emitted.target {
                    FlowTarget::Return => returned.push(emitted.message),
                    FlowTarget::Resident(_) => queue.push_back(emitted),
                }
            }
        }

        Ok(TurnOutput {
            snapshot_id: runtime.snapshot.id(),
            processed_packets,
            returned,
        })
    }
}

fn reusable_resident(
    current: Option<&RuntimeSet>,
    key: &ResidentKey,
    exposure_id: ExposureId,
) -> Option<ResidentBinding> {
    current?
        .residents
        .get(key)
        .filter(|binding| binding.exposure_id == exposure_id)
        .cloned()
}

fn reusable_gate(
    current: Option<&RuntimeSet>,
    key: &GateKey,
    exposure_id: ExposureId,
) -> Option<GateBinding> {
    current?
        .gates
        .iter()
        .find(|binding| binding.profile.key == *key && binding.exposure_id == exposure_id)
        .cloned()
}

async fn run_before_receive_hooks(
    runtime: &RuntimeSet,
    layout: Arc<dyn LayoutAccess>,
    resident: &ResidentKey,
    mut packet: FlowPacket,
) -> Result<BeforeReceiveOutcome, RuntimeError> {
    for binding in runtime
        .hooks
        .chain(resident, ResidentHookPoint::BeforeReceive)
    {
        let context = GateContext {
            layout: layout.clone(),
            resident: resident.clone(),
        };
        let action = binding
            .instance
            .lock()
            .await
            .before_receive(context, &packet)
            .await
            .map_err(|error| RuntimeError::GateFailure {
                gate: binding.profile.key.clone(),
                code: error.code,
                message: error.message,
            })?;
        match action {
            BeforeReceiveAction::Continue(message) => packet.message = message,
            BeforeReceiveAction::Drop => return Ok(BeforeReceiveOutcome::Drop),
            BeforeReceiveAction::Redirect(target) => {
                return Ok(BeforeReceiveOutcome::Redirect(packet.redirected_to(target)));
            }
        }
    }
    Ok(BeforeReceiveOutcome::Continue(packet))
}

enum BeforeReceiveOutcome {
    Continue(FlowPacket),
    Drop,
    Redirect(FlowPacket),
}

async fn run_effect_hooks(
    runtime: &RuntimeSet,
    layout: Arc<dyn LayoutAccess>,
    resident: &ResidentKey,
    point: ResidentHookPoint,
    packet: &FlowPacket,
    mut effect: ResidentEffect,
) -> Result<ResidentEffect, RuntimeError> {
    debug_assert!(matches!(
        point,
        ResidentHookPoint::AfterExecute | ResidentHookPoint::BeforeCommit
    ));
    for binding in runtime.hooks.chain(resident, point) {
        let context = GateContext {
            layout: layout.clone(),
            resident: resident.clone(),
        };
        let result = match point {
            ResidentHookPoint::AfterExecute => {
                binding
                    .instance
                    .lock()
                    .await
                    .after_execute(context, packet, effect)
                    .await
            }
            ResidentHookPoint::BeforeCommit => {
                binding
                    .instance
                    .lock()
                    .await
                    .before_commit(context, packet, effect)
                    .await
            }
            ResidentHookPoint::BeforeReceive | ResidentHookPoint::OnFailure => {
                unreachable!("effect hooks are only called for AfterExecute and BeforeCommit")
            }
        };
        effect = result.map_err(|error| RuntimeError::GateFailure {
            gate: binding.profile.key.clone(),
            code: error.code,
            message: error.message,
        })?;
    }
    Ok(effect)
}

async fn resolve_resident_failure(
    runtime: &RuntimeSet,
    layout: Arc<dyn LayoutAccess>,
    resident: &ResidentKey,
    failure: RuntimeFailure,
) -> Result<FailureOutcome, RuntimeError> {
    for binding in runtime.hooks.chain(resident, ResidentHookPoint::OnFailure) {
        let resolution = binding
            .instance
            .lock()
            .await
            .on_failure(
                GateContext {
                    layout: layout.clone(),
                    resident: resident.clone(),
                },
                failure.clone(),
            )
            .await
            .map_err(|error| RuntimeError::GateFailure {
                gate: binding.profile.key.clone(),
                code: error.code,
                message: error.message,
            })?;
        match resolution {
            FailureResolution::Propagate => {}
            FailureResolution::Drop => return Ok(FailureOutcome::Drop),
            FailureResolution::Redirect(packet) => {
                return Ok(FailureOutcome::Redirect(packet));
            }
        }
    }
    Ok(FailureOutcome::Unhandled)
}

enum FailureOutcome {
    Unhandled,
    Drop,
    Redirect(FlowPacket),
}

async fn commit_transitions(
    context: &Arc<RwLock<AgentThreadContext>>,
    snapshot: &RegistrationSnapshot,
    resident: &ResidentKey,
    events: Vec<StateEvent>,
) -> Result<(), RuntimeError> {
    if events.is_empty() {
        return Ok(());
    }
    let definitions = snapshot.machine_definitions();
    let mut context = context.write().await;
    let mut staged = context.machine_states.clone();
    for event in events {
        let Some(definition) = definitions.get(&event.machine) else {
            return Err(RuntimeError::ForeignStateMachine {
                resident: resident.clone(),
                machine: event.machine,
            });
        };
        if &definition.owner != resident {
            return Err(RuntimeError::ForeignStateMachine {
                resident: resident.clone(),
                machine: event.machine,
            });
        }
        let current = staged
            .get(&definition.key)
            .cloned()
            .unwrap_or_else(|| definition.initial_state.clone());
        let next = definition.next_state(&current, &event.event)?;
        staged.insert(definition.key.clone(), next);
    }
    context.machine_states = staged;
    Ok(())
}

async fn shutdown_created_residents(messengers: Vec<ResidentMessenger>) {
    for messenger in messengers {
        messenger.shutdown().await;
    }
}

async fn shutdown_created_gates(instances: Vec<GateInstance>) {
    for instance in instances {
        let _ = instance.lock().await.shutdown().await;
    }
}

async fn shutdown_replaced(previous: &RuntimeSet, current: &RuntimeSet) {
    for (key, binding) in &previous.residents {
        let retained = current
            .residents
            .get(key)
            .is_some_and(|candidate| binding.messenger.same_mailbox(&candidate.messenger));
        if !retained {
            binding.messenger.shutdown().await;
        }
    }
    for binding in &previous.gates {
        let retained = current
            .gates
            .iter()
            .find(|candidate| candidate.profile.key == binding.profile.key)
            .is_some_and(|candidate| Arc::ptr_eq(&binding.instance, &candidate.instance));
        if !retained {
            let _ = binding.instance.lock().await.shutdown().await;
        }
    }
}

async fn shutdown_runtime(runtime: RuntimeSet) {
    for binding in runtime.residents.values() {
        binding.messenger.shutdown().await;
    }
    for binding in &runtime.gates {
        let _ = binding.instance.lock().await.shutdown().await;
    }
}
