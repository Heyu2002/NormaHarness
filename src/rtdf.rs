use std::{fmt, sync::Arc};

use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    FlowMessage, GateContext, Rdf, ResidentEvent, ResidentInstanceId, ResidentKey, RouteError,
    RoutedMessage,
};

struct RouteCommand {
    source_instance: ResidentInstanceId,
    target: ResidentKey,
    message: FlowMessage,
    response: oneshot::Sender<Result<(), RouteError>>,
}

/// The narrow RTDF command port injected into a Resident.
///
/// This handle owns only a channel sender. It does not own RTDF, RDF, or
/// ResidentStore.
#[derive(Clone)]
pub struct MessageSender {
    commands: mpsc::UnboundedSender<RouteCommand>,
}

impl MessageSender {
    /// Sends one message through source outbound Gate, target inbound Gate,
    /// and target mailbox.
    pub async fn send(
        &self,
        source_instance: ResidentInstanceId,
        target: &ResidentKey,
        message: FlowMessage,
    ) -> Result<(), RouteError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(RouteCommand {
                source_instance,
                target: target.clone(),
                message,
                response,
            })
            .map_err(|_| RouteError::PipelineUnavailable)?;
        result.await.map_err(|_| RouteError::PipelineUnavailable)?
    }
}

impl fmt::Debug for MessageSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageSender")
            .finish_non_exhaustive()
    }
}

struct RtdfCore {
    rdf: Arc<Rdf>,
    shutdown: watch::Sender<bool>,
}

impl RtdfCore {
    async fn route(&self, command: RouteCommand) {
        let RouteCommand {
            source_instance,
            target,
            message,
            response,
        } = command;
        let _ = response.send(self.deliver(source_instance, &target, message).await);
    }

    async fn deliver(
        &self,
        source_instance: ResidentInstanceId,
        target: &ResidentKey,
        message: FlowMessage,
    ) -> Result<(), RouteError> {
        let route = self.rdf.resolve_route(source_instance, target).await?;
        let context = GateContext::new(route.source_key.clone(), route.target_key.clone());

        let message = if let Some(gate) = route.source_outbound_gate {
            gate.pass(&context, message)
                .await
                .map_err(|source| RouteError::OutboundGate {
                    resident: route.source_key.clone(),
                    source,
                })?
        } else {
            message
        };

        let message = if let Some(gate) = route.target_inbound_gate {
            gate.pass(&context, message)
                .await
                .map_err(|source| RouteError::InboundGate {
                    resident: route.target_key.clone(),
                    source,
                })?
        } else {
            message
        };

        let event = ResidentEvent::Message(RoutedMessage::new(
            route.source_key,
            route.target_key.clone(),
            message,
        ));
        route
            .target_mailbox
            .deliver(event)
            .map_err(|source| RouteError::TargetMailbox {
                resident: route.target_key,
                source,
            })
    }
}

impl Drop for RtdfCore {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

/// Runtime Data Flow: one-hop message delivery between registered Residents.
pub struct Rtdf {
    core: Arc<RtdfCore>,
    sender: MessageSender,
}

impl Rtdf {
    pub(crate) fn start(rdf: Arc<Rdf>) -> Self {
        let (command_sender, command_receiver) = mpsc::unbounded_channel();
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let core = Arc::new(RtdfCore { rdf, shutdown });
        Self::spawn_command_loop(&core, command_receiver, shutdown_receiver);
        Self {
            core,
            sender: MessageSender {
                commands: command_sender,
            },
        }
    }

    fn spawn_command_loop(
        core: &Arc<RtdfCore>,
        mut commands: mpsc::UnboundedReceiver<RouteCommand>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let core = Arc::downgrade(core);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    command = commands.recv() => {
                        let Some(command) = command else {
                            break;
                        };
                        let Some(core) = core.upgrade() else {
                            break;
                        };
                        tokio::spawn(async move {
                            core.route(command).await;
                        });
                    }
                }
            }
        });
    }

    #[must_use]
    pub fn message_sender(&self) -> MessageSender {
        self.sender.clone()
    }

    pub async fn send(
        &self,
        source_instance: ResidentInstanceId,
        target: &ResidentKey,
        message: FlowMessage,
    ) -> Result<(), RouteError> {
        self.sender.send(source_instance, target, message).await
    }
}

impl fmt::Debug for Rtdf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Rtdf")
            .field("rdf", &self.core.rdf)
            .finish_non_exhaustive()
    }
}
