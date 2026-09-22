use std::{collections::BTreeMap, fmt, sync::Arc};

use tokio::sync::{RwLock, mpsc, oneshot, watch};

use crate::resident_store::ResidentStore;
use crate::{
    CapabilityKey, Gate, MailboxAddress, RegistrationError, RegistrationNoticeError,
    RegistrationNoticeFailure, RegistrationReceipt, Resident, ResidentDescriptor, ResidentEvent,
    ResidentInstanceId, ResidentKey, RouteError,
};

#[derive(Clone)]
struct RegistrationRecord {
    instance_id: ResidentInstanceId,
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
    outbound_gate: Option<Arc<dyn Gate>>,
    inbound_gate: Option<Arc<dyn Gate>>,
}

#[derive(Default)]
struct RdfState {
    residents: BTreeMap<ResidentKey, RegistrationRecord>,
    by_instance: BTreeMap<ResidentInstanceId, ResidentKey>,
}

pub(crate) struct RouteResolution {
    pub(crate) source_key: ResidentKey,
    pub(crate) target_key: ResidentKey,
    pub(crate) source_outbound_gate: Option<Arc<dyn Gate>>,
    pub(crate) target_inbound_gate: Option<Arc<dyn Gate>>,
    pub(crate) target_mailbox: MailboxAddress,
}

enum RegistrationCommand {
    Register {
        resident: Arc<dyn Resident>,
        response: oneshot::Sender<Result<RegistrationReceipt, RegistrationError>>,
    },
    Unregister {
        instance_id: ResidentInstanceId,
        response: oneshot::Sender<Result<ResidentDescriptor, RegistrationError>>,
    },
}

/// The narrow command port injected into a Resident.
///
/// This handle owns only a channel sender. It does not own RDF or ResidentStore.
#[derive(Clone)]
pub struct RegistrationSender {
    commands: mpsc::UnboundedSender<RegistrationCommand>,
}

impl RegistrationSender {
    /// Injects the calling Resident into RDF's registration transaction.
    pub async fn register<R>(
        &self,
        resident: Arc<R>,
    ) -> Result<RegistrationReceipt, RegistrationError>
    where
        R: Resident,
    {
        let resident: Arc<dyn Resident> = resident;
        self.register_dyn(resident).await
    }

    /// Injects a dynamically typed Resident into RDF's registration transaction.
    pub async fn register_dyn(
        &self,
        resident: Arc<dyn Resident>,
    ) -> Result<RegistrationReceipt, RegistrationError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(RegistrationCommand::Register { resident, response })
            .map_err(|_| RegistrationError::PipelineUnavailable)?;
        result
            .await
            .map_err(|_| RegistrationError::PipelineUnavailable)?
    }

    /// Removes registration data and RDF's strong ownership after the Resident
    /// has stopped accepting and drained all of its own work.
    pub async fn unregister(
        &self,
        instance_id: ResidentInstanceId,
    ) -> Result<ResidentDescriptor, RegistrationError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(RegistrationCommand::Unregister {
                instance_id,
                response,
            })
            .map_err(|_| RegistrationError::PipelineUnavailable)?;
        result
            .await
            .map_err(|_| RegistrationError::PipelineUnavailable)?
    }
}

impl fmt::Debug for RegistrationSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistrationSender")
            .finish_non_exhaustive()
    }
}

/// Registration Data Flow: the sole authority for Resident names and capabilities.
///
/// RDF owns the private ResidentStore. Residents can reach RDF only through a
/// [`RegistrationSender`], so a stored Resident never owns RDF in return.
pub struct Rdf {
    resident_store: ResidentStore,
    state: RwLock<RdfState>,
    shutdown: watch::Sender<bool>,
}

impl Rdf {
    pub(crate) fn start() -> (Arc<Self>, RegistrationSender) {
        let (command_sender, command_receiver) = mpsc::unbounded_channel();
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let rdf = Arc::new(Self {
            resident_store: ResidentStore::new(),
            state: RwLock::new(RdfState::default()),
            shutdown,
        });
        Self::spawn_command_loop(&rdf, command_receiver, shutdown_receiver);
        (
            rdf,
            RegistrationSender {
                commands: command_sender,
            },
        )
    }

    fn spawn_command_loop(
        rdf: &Arc<Self>,
        mut commands: mpsc::UnboundedReceiver<RegistrationCommand>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let rdf = Arc::downgrade(rdf);
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
                        let Some(rdf) = rdf.upgrade() else {
                            break;
                        };
                        rdf.handle_command(command).await;
                    }
                }
            }
        });
    }

    async fn handle_command(&self, command: RegistrationCommand) {
        match command {
            RegistrationCommand::Register { resident, response } => {
                let _ = response.send(self.register_resident(resident).await);
            }
            RegistrationCommand::Unregister {
                instance_id,
                response,
            } => {
                let _ = response.send(self.unregister_resident(instance_id).await);
            }
        }
    }

    async fn register_resident(
        &self,
        resident: Arc<dyn Resident>,
    ) -> Result<RegistrationReceipt, RegistrationError> {
        let descriptor = resident.descriptor();
        let mailbox = resident.mailbox();
        let outbound_gate = resident.outbound_gate();
        let inbound_gate = resident.inbound_gate();
        let key = descriptor.key().clone();

        let (instance_id, existing_residents, existing_mailboxes) = {
            let mut state = self.state.write().await;

            if let Some(instance_id) = self.resident_store.instance_id(&resident) {
                let resident = state
                    .by_instance
                    .get(&instance_id)
                    .cloned()
                    .unwrap_or_else(|| key.clone());
                return Err(RegistrationError::InstanceAlreadyRegistered {
                    instance_id,
                    resident,
                });
            }

            let requested_instance = self.resident_store.reserve_instance_id()?;
            if let Some(existing) = state.residents.get(&key) {
                return Err(RegistrationError::DuplicateResident {
                    resident: key,
                    registered_instance: existing.instance_id,
                    requested_instance,
                });
            }

            let existing_residents = state
                .residents
                .values()
                .map(|record| record.descriptor.clone())
                .collect();
            let existing_mailboxes: Vec<(ResidentKey, MailboxAddress)> = state
                .residents
                .values()
                .map(|record| (record.descriptor.key().clone(), record.mailbox.clone()))
                .collect();

            self.resident_store.insert(requested_instance, resident);
            state.by_instance.insert(requested_instance, key.clone());
            state.residents.insert(
                key,
                RegistrationRecord {
                    instance_id: requested_instance,
                    descriptor: descriptor.clone(),
                    mailbox,
                    outbound_gate,
                    inbound_gate,
                },
            );

            (requested_instance, existing_residents, existing_mailboxes)
        };

        let announcement = ResidentEvent::ResidentRegistered(descriptor);
        let notice_failures = existing_mailboxes
            .into_iter()
            .filter_map(|(resident, mailbox)| {
                mailbox.deliver(announcement.clone()).err().map(|error| {
                    RegistrationNoticeFailure::new(
                        resident,
                        RegistrationNoticeError::Mailbox(error),
                    )
                })
            })
            .collect();

        Ok(RegistrationReceipt::new(
            instance_id,
            existing_residents,
            notice_failures,
        ))
    }

    async fn unregister_resident(
        &self,
        instance_id: ResidentInstanceId,
    ) -> Result<ResidentDescriptor, RegistrationError> {
        let (descriptor, resident) = {
            let mut state = self.state.write().await;
            let key = state
                .by_instance
                .remove(&instance_id)
                .ok_or(RegistrationError::UnknownRegistration { instance_id })?;
            let record = state
                .residents
                .remove(&key)
                .expect("instance and Resident indexes must change together");
            let resident = self
                .resident_store
                .remove(instance_id)
                .expect("registration and ResidentStore must change together");
            (record.descriptor, resident)
        };

        drop(resident);
        Ok(descriptor)
    }

    #[must_use]
    pub async fn is_registered(&self, instance_id: ResidentInstanceId) -> bool {
        self.state
            .read()
            .await
            .by_instance
            .contains_key(&instance_id)
    }

    #[must_use]
    pub async fn residents(&self) -> Vec<ResidentDescriptor> {
        self.state
            .read()
            .await
            .residents
            .values()
            .map(|record| record.descriptor.clone())
            .collect()
    }

    #[must_use]
    pub async fn resident(&self, key: &ResidentKey) -> Option<ResidentDescriptor> {
        self.state
            .read()
            .await
            .residents
            .get(key)
            .map(|record| record.descriptor.clone())
    }

    #[must_use]
    pub async fn providers(&self, capability: &CapabilityKey) -> Vec<ResidentDescriptor> {
        self.state
            .read()
            .await
            .residents
            .values()
            .filter(|record| record.descriptor.provides(capability))
            .map(|record| record.descriptor.clone())
            .collect()
    }

    #[must_use]
    pub async fn resident_count(&self) -> usize {
        self.state.read().await.residents.len()
    }

    #[must_use]
    pub async fn stored_instance_count(&self) -> usize {
        let _state = self.state.read().await;
        self.resident_store.len()
    }

    pub(crate) async fn resolve_route(
        &self,
        source_instance: ResidentInstanceId,
        target: &ResidentKey,
    ) -> Result<RouteResolution, RouteError> {
        let state = self.state.read().await;
        let source_key =
            state
                .by_instance
                .get(&source_instance)
                .ok_or(RouteError::UnknownSource {
                    instance_id: source_instance,
                })?;
        let source = state
            .residents
            .get(source_key)
            .expect("instance and Resident indexes must change together");
        let target_record =
            state
                .residents
                .get(target)
                .ok_or_else(|| RouteError::UnknownTarget {
                    resident: target.clone(),
                })?;

        Ok(RouteResolution {
            source_key: source_key.clone(),
            target_key: target.clone(),
            source_outbound_gate: source.outbound_gate.clone(),
            target_inbound_gate: target_record.inbound_gate.clone(),
            target_mailbox: target_record.mailbox.clone(),
        })
    }
}

impl Drop for Rdf {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

impl fmt::Debug for Rdf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Rdf")
            .field("resident_store", &self.resident_store)
            .finish_non_exhaustive()
    }
}
