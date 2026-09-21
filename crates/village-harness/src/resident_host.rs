use tokio::sync::{mpsc, oneshot};
use village_harness_protocol::{
    ResidentEffect, ResidentError, ResidentInvocation, ResidentKey, ResidentResponse,
};
use wasmi::{
    Config, Engine, Instance, Linker, Memory, Module, Store, StoreLimits, StoreLimitsBuilder,
    TypedFunc,
};

use crate::{
    NativeResidentRegistration, RegistrationError, Resident, ResidentArtifact, ResidentContext,
    ResidentLimits,
};

const ABI_MEMORY: &str = "memory";
const ABI_VERSION: &str = "village_abi_version";
const SUPPORTED_ABI_VERSION: i32 = 1;
const ABI_ALLOC: &str = "village_alloc";
const ABI_RECEIVE: &str = "village_receive";
const ABI_DEALLOC: &str = "village_dealloc";
const ABI_SHUTDOWN: &str = "village_shutdown";

#[derive(Clone)]
pub(crate) struct CompiledResident {
    engine: Engine,
    module: Module,
    limits: ResidentLimits,
}

impl CompiledResident {
    fn compile(artifact: &ResidentArtifact) -> Result<Self, RegistrationError> {
        let resident = artifact.profile().key.clone();
        validate_limits(resident.clone(), artifact.limits())?;

        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let module = Module::new(&engine, artifact.wasm()).map_err(|error| {
            RegistrationError::InvalidResidentArtifact {
                resident: resident.clone(),
                message: error.to_string(),
            }
        })?;
        if let Some(import) = module.imports().next() {
            return Err(RegistrationError::ResidentImportForbidden {
                resident,
                module: import.module().to_owned(),
                name: import.name().to_owned(),
            });
        }
        Ok(Self {
            engine,
            module,
            limits: artifact.limits(),
        })
    }
}

impl std::fmt::Debug for CompiledResident {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledResident")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ResidentDriver {
    Wasm(CompiledResident),
    Native(NativeResidentRegistration),
}

impl ResidentDriver {
    pub(crate) fn compile_wasm(artifact: &ResidentArtifact) -> Result<Self, RegistrationError> {
        CompiledResident::compile(artifact).map(Self::Wasm)
    }

    pub(crate) fn native(
        registration: NativeResidentRegistration,
    ) -> Result<Self, RegistrationError> {
        if registration.mailbox_capacity() == 0 {
            return Err(RegistrationError::InvalidResidentArtifact {
                resident: registration.profile().key.clone(),
                message: "native Resident mailbox capacity must be greater than zero".into(),
            });
        }
        Ok(Self::Native(registration))
    }

    pub(crate) fn instantiate(
        &self,
        resident: ResidentKey,
    ) -> Result<ResidentMessenger, ResidentHostError> {
        let (instance, mailbox_capacity) = match self {
            Self::Wasm(compiled) => (
                ResidentInstance::Wasm(Box::new(WasmResident::new(compiled)?)),
                compiled.limits.mailbox_capacity,
            ),
            Self::Native(registration) => (
                ResidentInstance::Native(registration.instantiate()),
                registration.mailbox_capacity(),
            ),
        };
        let (sender, receiver) = mpsc::channel(mailbox_capacity);
        tokio::spawn(run_resident_mailbox(instance, receiver));
        Ok(ResidentMessenger { resident, sender })
    }
}

fn validate_limits(resident: ResidentKey, limits: ResidentLimits) -> Result<(), RegistrationError> {
    if limits.max_memory_bytes == 0
        || limits.max_input_bytes == 0
        || limits.max_output_bytes == 0
        || limits.fuel_per_delivery == 0
        || limits.mailbox_capacity == 0
    {
        return Err(RegistrationError::InvalidResidentArtifact {
            resident,
            message: "all Resident limits must be greater than zero".into(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct ResidentMessenger {
    resident: ResidentKey,
    sender: mpsc::Sender<ResidentCommand>,
}

impl ResidentMessenger {
    pub(crate) async fn deliver(
        &self,
        invocation: ResidentInvocation,
    ) -> Result<ResidentEffect, ResidentError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ResidentCommand::Deliver {
                invocation: Box::new(invocation),
                reply,
            })
            .await
            .map_err(|_| {
                ResidentError::new(
                    "RESIDENT_MAILBOX_CLOSED",
                    format!("Resident {} mailbox is closed", self.resident),
                )
            })?;
        response.await.map_err(|_| {
            ResidentError::new(
                "RESIDENT_MAILBOX_CLOSED",
                format!("Resident {} stopped before replying", self.resident),
            )
        })?
    }

    pub(crate) async fn shutdown(&self) {
        let (reply, response) = oneshot::channel();
        if self
            .sender
            .send(ResidentCommand::Shutdown { reply })
            .await
            .is_ok()
        {
            let _ = response.await;
        }
    }

    pub(crate) fn same_mailbox(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }
}

#[derive(Debug)]
enum ResidentCommand {
    Deliver {
        invocation: Box<ResidentInvocation>,
        reply: oneshot::Sender<Result<ResidentEffect, ResidentError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

async fn run_resident_mailbox(
    mut resident: ResidentInstance,
    mut receiver: mpsc::Receiver<ResidentCommand>,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            ResidentCommand::Deliver { invocation, reply } => {
                let _ = reply.send(resident.deliver(*invocation).await);
            }
            ResidentCommand::Shutdown { reply } => {
                resident.shutdown().await;
                let _ = reply.send(());
                break;
            }
        }
    }
}

enum ResidentInstance {
    Wasm(Box<WasmResident>),
    Native(Box<dyn Resident>),
}

impl ResidentInstance {
    async fn deliver(
        &mut self,
        invocation: ResidentInvocation,
    ) -> Result<ResidentEffect, ResidentError> {
        match self {
            Self::Wasm(resident) => resident.deliver(&invocation),
            Self::Native(resident) => {
                let mut context = ResidentContext::new(invocation);
                resident.handle(&mut context).await?;
                Ok(context.into_effect())
            }
        }
    }

    async fn shutdown(&mut self) {
        match self {
            Self::Wasm(resident) => resident.shutdown(),
            Self::Native(resident) => {
                let _ = resident.shutdown().await;
            }
        }
    }
}

struct WasmResident {
    store: Store<StoreLimits>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    receive: TypedFunc<(i32, i32), i64>,
    dealloc: TypedFunc<(i32, i32), ()>,
    shutdown: Option<TypedFunc<(), ()>>,
    limits: ResidentLimits,
}

impl WasmResident {
    fn new(compiled: &CompiledResident) -> Result<Self, ResidentHostError> {
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(compiled.limits.max_memory_bytes)
            .memories(1)
            .tables(1)
            .instances(1)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&compiled.engine, store_limits);
        store.limiter(|limits| limits);
        store
            .set_fuel(compiled.limits.fuel_per_delivery)
            .map_err(|error| ResidentHostError::new("RESIDENT_FUEL", error))?;
        let instance = Linker::<StoreLimits>::new(&compiled.engine)
            .instantiate(&mut store, &compiled.module)
            .and_then(|pre| pre.start(&mut store))
            .map_err(|error| ResidentHostError::new("RESIDENT_INSTANTIATION", error))?;
        Self::from_instance(store, instance, compiled.limits)
    }

    fn from_instance(
        mut store: Store<StoreLimits>,
        instance: Instance,
        limits: ResidentLimits,
    ) -> Result<Self, ResidentHostError> {
        let abi_version = instance
            .get_typed_func::<(), i32>(&store, ABI_VERSION)
            .map_err(|error| ResidentHostError::abi(error.to_string()))?;
        let reported_version = abi_version
            .call(&mut store, ())
            .map_err(|error| ResidentHostError::new("RESIDENT_ABI", error))?;
        if reported_version != SUPPORTED_ABI_VERSION {
            return Err(ResidentHostError::abi(format!(
                "unsupported ABI version {reported_version}; expected {SUPPORTED_ABI_VERSION}"
            )));
        }
        let memory = instance
            .get_memory(&store, ABI_MEMORY)
            .ok_or_else(|| ResidentHostError::abi("missing exported memory"))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&store, ABI_ALLOC)
            .map_err(|error| ResidentHostError::abi(error.to_string()))?;
        let receive = instance
            .get_typed_func::<(i32, i32), i64>(&store, ABI_RECEIVE)
            .map_err(|error| ResidentHostError::abi(error.to_string()))?;
        let dealloc = instance
            .get_typed_func::<(i32, i32), ()>(&store, ABI_DEALLOC)
            .map_err(|error| ResidentHostError::abi(error.to_string()))?;
        let shutdown = instance.get_typed_func::<(), ()>(&store, ABI_SHUTDOWN).ok();
        Ok(Self {
            store,
            memory,
            alloc,
            receive,
            dealloc,
            shutdown,
            limits,
        })
    }

    fn deliver(
        &mut self,
        invocation: &ResidentInvocation,
    ) -> Result<ResidentEffect, ResidentError> {
        self.deliver_inner(invocation)
            .map_err(ResidentHostError::into_resident_error)
    }

    fn deliver_inner(
        &mut self,
        invocation: &ResidentInvocation,
    ) -> Result<ResidentEffect, ResidentHostError> {
        let input = serde_json::to_vec(invocation)
            .map_err(|error| ResidentHostError::new("RESIDENT_INPUT_ENCODING", error))?;
        if input.len() > self.limits.max_input_bytes {
            return Err(ResidentHostError::with_message(
                "RESIDENT_INPUT_LIMIT",
                format!(
                    "encoded invocation is {} bytes; limit is {}",
                    input.len(),
                    self.limits.max_input_bytes
                ),
            ));
        }
        let input_len = i32::try_from(input.len()).map_err(|_| {
            ResidentHostError::with_message("RESIDENT_INPUT_LIMIT", "invocation length exceeds i32")
        })?;
        self.store
            .set_fuel(self.limits.fuel_per_delivery)
            .map_err(|error| ResidentHostError::new("RESIDENT_FUEL", error))?;
        let input_ptr = self
            .alloc
            .call(&mut self.store, input_len)
            .map_err(|error| ResidentHostError::new("RESIDENT_TRAP", error))?;
        let input_offset = usize::try_from(input_ptr)
            .map_err(|_| ResidentHostError::abi("allocator returned a negative pointer"))?;
        self.memory
            .write(&mut self.store, input_offset, &input)
            .map_err(|error| ResidentHostError::new("RESIDENT_MEMORY", error))?;

        let packed = self
            .receive
            .call(&mut self.store, (input_ptr, input_len))
            .map_err(|error| ResidentHostError::new("RESIDENT_TRAP", error));
        let _ = self.dealloc.call(&mut self.store, (input_ptr, input_len));
        let packed = packed? as u64;
        let output_ptr = (packed >> 32) as u32 as usize;
        let output_len = (packed & u64::from(u32::MAX)) as u32 as usize;
        if output_len > self.limits.max_output_bytes {
            return Err(ResidentHostError::with_message(
                "RESIDENT_OUTPUT_LIMIT",
                format!(
                    "Resident returned {output_len} bytes; limit is {}",
                    self.limits.max_output_bytes
                ),
            ));
        }
        let mut output = vec![0; output_len];
        self.memory
            .read(&self.store, output_ptr, &mut output)
            .map_err(|error| ResidentHostError::new("RESIDENT_MEMORY", error))?;
        let output_ptr_i32 = i32::try_from(output_ptr)
            .map_err(|_| ResidentHostError::abi("output pointer exceeds i32"))?;
        let output_len_i32 = i32::try_from(output_len)
            .map_err(|_| ResidentHostError::abi("output length exceeds i32"))?;
        let _ = self
            .dealloc
            .call(&mut self.store, (output_ptr_i32, output_len_i32));

        match serde_json::from_slice::<ResidentResponse>(&output)
            .map_err(|error| ResidentHostError::new("RESIDENT_OUTPUT_DECODING", error))?
        {
            ResidentResponse::Ok(effect) => Ok(effect),
            ResidentResponse::Error(error) => Err(ResidentHostError::from_resident(error)),
        }
    }

    fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown {
            let _ = self.store.set_fuel(self.limits.fuel_per_delivery);
            let _ = shutdown.call(&mut self.store, ());
        }
    }
}

#[derive(Debug)]
pub(crate) struct ResidentHostError {
    code: String,
    message: String,
    retryable: bool,
}

impl ResidentHostError {
    fn new(code: impl Into<String>, error: impl std::fmt::Display) -> Self {
        Self::with_message(code, error.to_string())
    }

    fn with_message(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }

    fn abi(message: impl Into<String>) -> Self {
        Self::with_message("RESIDENT_ABI", message)
    }

    fn from_resident(error: ResidentError) -> Self {
        Self {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
        }
    }

    pub(crate) fn code(&self) -> &str {
        &self.code
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    fn into_resident_error(self) -> ResidentError {
        ResidentError {
            code: self.code,
            message: self.message,
            retryable: self.retryable,
        }
    }
}
