use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;
use village_harness_protocol::{
    CapabilityKey, Emission, MessageKind, ResidentError, ResidentKey, ResidentProfile,
    TOOL_CANCEL_REQUEST_KIND, TOOL_INVOKE_REQUEST_KIND, TOOL_LIST_REQUEST_KIND, ToolCancelRequest,
    ToolCatalog, ToolDescriptor, ToolError, ToolInvokeRequest, ToolKey, ToolListRequest,
    ToolResult,
};

use crate::{NativeResidentRegistration, Resident, ResidentContext};

/// One business operation managed inside a [`ToolResident`].
///
/// Tool-specific validation, cleanup, and business errors belong here. Gates
/// remain mounted to the ToolResident as a whole and are never dispatched per
/// Tool key by the runtime.
#[async_trait]
pub trait Tool: Send + 'static {
    async fn invoke(&mut self, arguments: Value) -> Result<Value, ToolError>;

    /// Best-effort cancellation hook.
    ///
    /// The current Resident mailbox is serial, so this cannot interrupt an
    /// invocation that is already executing in the same ToolResident. Tools
    /// that manage background work may override it to cancel that work.
    async fn cancel(&mut self, _call_id: Uuid) -> Result<(), ToolError> {
        Err(ToolError::new(
            "TOOL_CANCELLATION_UNSUPPORTED",
            "this tool does not support cancellation",
        ))
    }
}

type ToolFactory = Arc<dyn Fn() -> Box<dyn Tool> + Send + Sync>;

#[derive(Clone)]
struct ToolDefinition {
    descriptor: ToolDescriptor,
    factory: ToolFactory,
}

impl std::fmt::Debug for ToolDefinition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolDefinition")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

struct ToolEntry {
    descriptor: ToolDescriptor,
    instance: Box<dyn Tool>,
}

/// A trusted native Resident whose Tool catalog and implementations are fully
/// encapsulated inside the Resident.
pub struct ToolResident {
    tools: BTreeMap<ToolKey, ToolEntry>,
}

impl ToolResident {
    pub fn builder(key: ResidentKey) -> ToolResidentBuilder {
        ToolResidentBuilder::new(key)
    }

    fn from_definitions(definitions: &BTreeMap<ToolKey, ToolDefinition>) -> Self {
        let tools = definitions
            .iter()
            .map(|(key, definition)| {
                (
                    key.clone(),
                    ToolEntry {
                        descriptor: definition.descriptor.clone(),
                        instance: (definition.factory)(),
                    },
                )
            })
            .collect();
        Self { tools }
    }

    fn catalog(&self) -> ToolCatalog {
        ToolCatalog::new(
            self.tools
                .values()
                .map(|entry| entry.descriptor.clone())
                .collect(),
        )
    }
}

impl std::fmt::Debug for ToolResident {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolResident")
            .field("tools", &self.tools.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[async_trait]
impl Resident for ToolResident {
    async fn handle(&mut self, context: &mut ResidentContext) -> Result<(), ResidentError> {
        let kind = context.message().kind.as_str();
        let payload = context.message().payload.clone();
        match kind {
            TOOL_LIST_REQUEST_KIND => {
                let request: ToolListRequest = decode_request(payload)?;
                context.emit(Emission {
                    target: request.reply_to,
                    message: self.catalog().into_message(),
                });
            }
            TOOL_INVOKE_REQUEST_KIND => {
                let request: ToolInvokeRequest = decode_request(payload)?;
                let result = match self.tools.get_mut(&request.tool) {
                    Some(tool) => match tool.instance.invoke(request.arguments).await {
                        Ok(value) => {
                            ToolResult::success(request.call_id, request.tool.clone(), value)
                        }
                        Err(error) => {
                            ToolResult::error(request.call_id, request.tool.clone(), error)
                        }
                    },
                    None => ToolResult::error(
                        request.call_id,
                        request.tool.clone(),
                        ToolError::new(
                            "TOOL_NOT_FOUND",
                            format!("Tool {} is not registered", request.tool),
                        ),
                    ),
                };
                context.emit(Emission {
                    target: request.reply_to,
                    message: result.into_message(),
                });
            }
            TOOL_CANCEL_REQUEST_KIND => {
                let request: ToolCancelRequest = decode_request(payload)?;
                let result = match self.tools.get_mut(&request.tool) {
                    Some(tool) => match tool.instance.cancel(request.call_id).await {
                        Ok(()) => ToolResult::cancelled(request.call_id, request.tool.clone()),
                        Err(error) => {
                            ToolResult::error(request.call_id, request.tool.clone(), error)
                        }
                    },
                    None => ToolResult::error(
                        request.call_id,
                        request.tool.clone(),
                        ToolError::new(
                            "TOOL_NOT_FOUND",
                            format!("Tool {} is not registered", request.tool),
                        ),
                    ),
                };
                context.emit(Emission {
                    target: request.reply_to,
                    message: result.into_message(),
                });
            }
            _ => {
                return Err(ResidentError::new(
                    "TOOL_PROTOCOL",
                    format!("unsupported ToolResident message kind {kind}"),
                ));
            }
        }
        Ok(())
    }
}

fn decode_request<T>(payload: Value) -> Result<T, ResidentError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(payload).map_err(|error| {
        ResidentError::new(
            "TOOL_PROTOCOL",
            format!("invalid ToolResident request: {error}"),
        )
    })
}

#[derive(Debug)]
pub struct ToolResidentBuilder {
    key: ResidentKey,
    definitions: Vec<ToolDefinition>,
}

impl ToolResidentBuilder {
    pub fn new(key: ResidentKey) -> Self {
        Self {
            key,
            definitions: Vec::new(),
        }
    }

    pub fn tool<T, F>(mut self, descriptor: ToolDescriptor, factory: F) -> Self
    where
        T: Tool,
        F: Fn() -> T + Send + Sync + 'static,
    {
        let factory = Arc::new(move || Box::new(factory()) as Box<dyn Tool>);
        self.definitions.push(ToolDefinition {
            descriptor,
            factory,
        });
        self
    }

    pub fn build(self) -> Result<NativeResidentRegistration, ToolResidentBuildError> {
        let mut definitions = BTreeMap::new();
        for definition in self.definitions {
            let key = definition.descriptor.key.clone();
            if definitions.insert(key.clone(), definition).is_some() {
                return Err(ToolResidentBuildError::DuplicateTool(key));
            }
        }

        let profile = ResidentProfile {
            key: self.key,
            accepted_messages: [
                TOOL_LIST_REQUEST_KIND,
                TOOL_INVOKE_REQUEST_KIND,
                TOOL_CANCEL_REQUEST_KIND,
            ]
            .into_iter()
            .map(|kind| MessageKind::new(kind).expect("built-in Tool message kind is valid"))
            .collect(),
            provided_capabilities: [
                CapabilityKey::new("tools.invoke").expect("built-in Tool capability is valid")
            ]
            .into_iter()
            .collect(),
            state_machines: Vec::new(),
        };
        let definitions = Arc::new(definitions);
        Ok(NativeResidentRegistration::new(profile, move || {
            ToolResident::from_definitions(&definitions)
        }))
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ToolResidentBuildError {
    #[error("ToolResident contains more than one Tool named {0}")]
    DuplicateTool(ToolKey),
}
