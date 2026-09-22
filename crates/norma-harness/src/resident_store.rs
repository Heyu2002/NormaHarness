use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{RegistrationError, Resident, ResidentInstanceId};

#[derive(Default)]
struct ResidentStoreState {
    next_instance_id: u64,
    instances: BTreeMap<ResidentInstanceId, Arc<dyn Resident>>,
}

/// RDF-owned storage for successfully registered Resident instances.
pub(crate) struct ResidentStore {
    state: RwLock<ResidentStoreState>,
}

impl ResidentStore {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            state: RwLock::new(ResidentStoreState::default()),
        }
    }

    pub(crate) fn reserve_instance_id(&self) -> Result<ResidentInstanceId, RegistrationError> {
        let mut state = self.write_state();
        let value = state
            .next_instance_id
            .checked_add(1)
            .ok_or(RegistrationError::InstanceIdExhausted)?;
        state.next_instance_id = value;
        Ok(ResidentInstanceId::new(value))
    }

    pub(crate) fn insert(&self, instance_id: ResidentInstanceId, resident: Arc<dyn Resident>) {
        let previous = self.write_state().instances.insert(instance_id, resident);
        debug_assert!(previous.is_none(), "Resident instance IDs are never reused");
    }

    pub(crate) fn instance_id(&self, resident: &Arc<dyn Resident>) -> Option<ResidentInstanceId> {
        self.read_state()
            .instances
            .iter()
            .find_map(|(instance_id, stored)| Arc::ptr_eq(stored, resident).then_some(*instance_id))
    }

    pub(crate) fn remove(&self, instance_id: ResidentInstanceId) -> Option<Arc<dyn Resident>> {
        self.write_state().instances.remove(&instance_id)
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.read_state().instances.len()
    }

    fn read_state(&self) -> RwLockReadGuard<'_, ResidentStoreState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, ResidentStoreState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for ResidentStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentStore")
            .field("resident_count", &self.len())
            .finish()
    }
}
