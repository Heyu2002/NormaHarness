//! Opt-in, Resident-owned long-term memory. Raw facts remain on disk when their
//! cache or hot status changes.

use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::PathBuf,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::llm::LlmContextMessage;

const FREQUENT_WINDOW_MS: u64 = 48 * 60 * 60 * 1000;
const CACHE_DWELL_MS: u64 = 24 * 60 * 60 * 1000;
const HOT_IDLE_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const HOT_CAPACITY: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryFact {
    pub key: String,
    pub text: String,
    pub source_message_ids: Vec<u64>,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub recent_mentions_ms: Vec<u64>,
    pub cache_since_ms: Option<u64>,
    pub hot_since_ms: Option<u64>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MemoryState {
    enabled: bool,
    facts: Vec<MemoryFact>,
    cursors: HashMap<u64, u64>,
    revision: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryStatus {
    pub enabled: bool,
    pub stored: usize,
    pub cached: usize,
    pub hot: usize,
}

#[derive(Debug, Deserialize)]
pub struct ExtractedMemory {
    #[serde(default)]
    pub facts: Vec<ExtractedFact>,
}

#[derive(Debug, Deserialize)]
pub struct ExtractedFact {
    pub key: String,
    pub text: String,
    #[serde(default)]
    pub evidence_ids: Vec<u64>,
}

#[derive(Debug)]
pub struct MemoryManager {
    path: PathBuf,
    state: Mutex<MemoryState>,
}

impl MemoryManager {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let backup = path.with_extension("json.bak");
        let state = match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(state) => state,
                Err(error) if backup.exists() => {
                    eprintln!("memory file is invalid ({error}); trying backup");
                    serde_json::from_slice(&fs::read(backup)?)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                }
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound && backup.exists() => {
                serde_json::from_slice(&fs::read(backup)?)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => MemoryState::default(),
            Err(error) => return Err(error),
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    pub fn status(&self) -> MemoryStatus {
        let state = self.state.lock().expect("memory mutex");
        MemoryStatus {
            enabled: state.enabled,
            stored: state.facts.len(),
            cached: state
                .facts
                .iter()
                .filter(|fact| fact.cache_since_ms.is_some())
                .count(),
            hot: state
                .facts
                .iter()
                .filter(|fact| fact.hot_since_ms.is_some())
                .count(),
        }
    }

    pub fn set_enabled(&self, enabled: bool) -> io::Result<MemoryStatus> {
        let mut state = self.state.lock().expect("memory mutex");
        let mut next = state.clone();
        next.enabled = enabled;
        next.revision += 1;
        self.save(&next)?;
        *state = next;
        Ok(MemoryStatus {
            enabled,
            stored: state.facts.len(),
            cached: state
                .facts
                .iter()
                .filter(|fact| fact.cache_since_ms.is_some())
                .count(),
            hot: state
                .facts
                .iter()
                .filter(|fact| fact.hot_since_ms.is_some())
                .count(),
        })
    }

    pub fn unprocessed(
        &self,
        room_id: u64,
        context: &[LlmContextMessage],
    ) -> Vec<LlmContextMessage> {
        let state = self.state.lock().expect("memory mutex");
        if !state.enabled {
            return Vec::new();
        }
        let cursor = state.cursors.get(&room_id).copied().unwrap_or(0);
        context
            .iter()
            .filter(|event| event.id > cursor)
            .cloned()
            .collect()
    }

    pub fn known_keys(&self) -> Vec<String> {
        let state = self.state.lock().expect("memory mutex");
        state
            .facts
            .iter()
            .map(|fact| fact.key.clone())
            .take(200)
            .collect()
    }

    pub fn hot(&self) -> io::Result<(u64, Vec<String>)> {
        let mut state = self.state.lock().expect("memory mutex");
        if !state.enabled {
            return Ok((state.revision, Vec::new()));
        }
        let mut next = state.clone();
        if rebalance(&mut next, now_ms()) {
            self.save(&next)?;
            *state = next;
        }
        Ok((
            state.revision,
            state
                .facts
                .iter()
                .filter(|fact| fact.hot_since_ms.is_some())
                .map(|fact| fact.text.clone())
                .collect(),
        ))
    }

    pub fn apply(
        &self,
        room_id: u64,
        events: &[LlmContextMessage],
        extracted: ExtractedMemory,
    ) -> io::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().expect("memory mutex");
        if !state.enabled {
            return Ok(());
        }
        let mut next = state.clone();
        let cursor = next.cursors.get(&room_id).copied().unwrap_or(0);
        let allowed: HashSet<u64> = events
            .iter()
            .filter(|event| event.id > cursor && event.role == "user")
            .map(|event| event.id)
            .collect();
        let now = now_ms();
        for item in extracted.facts {
            let key = item.key.trim().to_lowercase();
            let text = item.text.trim();
            if key.is_empty() || key.len() > 160 || text.is_empty() || text.len() > 500 {
                continue;
            }
            let evidence: HashSet<u64> = item
                .evidence_ids
                .into_iter()
                .filter(|id| allowed.contains(id))
                .collect();
            if evidence.is_empty() {
                continue;
            }
            let mentioned_at: Vec<u64> = evidence
                .iter()
                .filter_map(|id| events.iter().find(|event| event.id == *id))
                .map(|event| {
                    if event.created_at_ms == 0 {
                        now
                    } else {
                        event.created_at_ms
                    }
                })
                .collect();
            let first_seen = mentioned_at.iter().copied().min().unwrap_or(now);
            let last_seen = mentioned_at.iter().copied().max().unwrap_or(now);
            let fact = if let Some(existing) = next.facts.iter_mut().find(|fact| fact.key == key) {
                existing
            } else {
                next.facts.push(MemoryFact {
                    key,
                    text: text.to_owned(),
                    source_message_ids: Vec::new(),
                    first_seen_ms: first_seen,
                    last_seen_ms: last_seen,
                    recent_mentions_ms: Vec::new(),
                    cache_since_ms: None,
                    hot_since_ms: None,
                });
                next.facts.last_mut().expect("fact just inserted")
            };
            fact.text = text.to_owned();
            fact.first_seen_ms = fact.first_seen_ms.min(first_seen);
            fact.last_seen_ms = fact.last_seen_ms.max(last_seen);
            for id in evidence {
                if !fact.source_message_ids.contains(&id) {
                    fact.source_message_ids.push(id);
                    fact.recent_mentions_ms.push(
                        events
                            .iter()
                            .find(|event| event.id == id)
                            .map(|event| {
                                if event.created_at_ms == 0 {
                                    now
                                } else {
                                    event.created_at_ms
                                }
                            })
                            .unwrap_or(now),
                    );
                }
            }
            fact.recent_mentions_ms
                .retain(|time| now.saturating_sub(*time) <= FREQUENT_WINDOW_MS);
            if fact.cache_since_ms.is_none() && fact.recent_mentions_ms.len() >= 3 {
                fact.cache_since_ms = Some(now);
            }
        }
        if let Some(max_id) = events.iter().map(|event| event.id).max() {
            next.cursors.insert(room_id, max_id);
        }
        next.revision += 1;
        rebalance(&mut next, now);
        self.save(&next)?;
        *state = next;
        Ok(())
    }

    fn save(&self, state: &MemoryState) -> io::Result<()> {
        let pending = self.path.with_extension("json.tmp");
        let backup = self.path.with_extension("json.bak");
        let encoded = serde_json::to_vec(state)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut file = fs::File::create(&pending)?;
        use io::Write;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);
        if self.path.exists() {
            if backup.exists() {
                fs::remove_file(&backup)?;
            }
            fs::rename(&self.path, &backup)?;
        }
        if let Err(error) = fs::rename(&pending, &self.path) {
            if backup.exists() {
                let _ = fs::rename(&backup, &self.path);
            }
            return Err(error);
        }
        Ok(())
    }
}

fn rebalance(state: &mut MemoryState, now: u64) -> bool {
    let mut changed = false;
    for fact in &mut state.facts {
        let before = fact.recent_mentions_ms.len();
        fact.recent_mentions_ms
            .retain(|time| now.saturating_sub(*time) <= FREQUENT_WINDOW_MS);
        changed |= fact.recent_mentions_ms.len() != before;
        if fact.hot_since_ms.is_some() && now.saturating_sub(fact.last_seen_ms) >= HOT_IDLE_MS {
            fact.hot_since_ms = None;
            fact.cache_since_ms = None;
            changed = true;
        }
    }
    let candidates: Vec<usize> = state
        .facts
        .iter()
        .enumerate()
        .filter(|(_, fact)| {
            fact.hot_since_ms.is_none()
                && fact
                    .cache_since_ms
                    .is_some_and(|since| now.saturating_sub(since) >= CACHE_DWELL_MS)
        })
        .map(|(index, _)| index)
        .collect();
    for index in candidates {
        let hot_count = state
            .facts
            .iter()
            .filter(|fact| fact.hot_since_ms.is_some())
            .count();
        if hot_count >= HOT_CAPACITY {
            if let Some(worst) = state
                .facts
                .iter()
                .enumerate()
                .filter(|(_, fact)| fact.hot_since_ms.is_some())
                .min_by_key(|(_, fact)| (fact.last_seen_ms, fact.source_message_ids.len()))
                .map(|(index, _)| index)
            {
                if state.facts[worst].last_seen_ms >= state.facts[index].last_seen_ms {
                    continue;
                }
                state.facts[worst].hot_since_ms = None;
            }
        }
        state.facts[index].hot_since_ms = Some(now);
        changed = true;
    }
    if changed {
        state.revision += 1;
    }
    changed
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: u64, role: &str, time: u64) -> LlmContextMessage {
        LlmContextMessage {
            id,
            room_id: 1,
            room_name: "test".into(),
            role: role.into(),
            author: "you".into(),
            text: "prefers concise replies".into(),
            created_at_ms: time,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn memory_is_opt_in_and_deduplicates_processed_messages() {
        let path = std::env::temp_dir().join(format!(
            "norma-memory-test-{}-{}.json",
            std::process::id(),
            now_ms()
        ));
        let manager = MemoryManager::open(path.clone()).unwrap();
        let now = now_ms();
        let events = vec![
            event(1, "user", now - 2000),
            event(2, "user", now - 1000),
            event(3, "user", now),
            event(4, "agent", now),
        ];
        assert!(manager.unprocessed(1, &events).is_empty());
        manager.set_enabled(true).unwrap();
        manager
            .apply(
                1,
                &events,
                ExtractedMemory {
                    facts: vec![ExtractedFact {
                        key: "style".into(),
                        text: "Prefers concise replies".into(),
                        evidence_ids: vec![1, 2, 3, 4],
                    }],
                },
            )
            .unwrap();
        assert_eq!(manager.status().stored, 1);
        assert_eq!(manager.status().cached, 1);
        assert_eq!(manager.status().hot, 0);
        assert!(manager.unprocessed(1, &events).is_empty());
        assert_eq!(
            manager.state.lock().unwrap().facts[0]
                .source_message_ids
                .len(),
            3
        );
        let restored = MemoryManager::open(path.clone()).unwrap();
        assert_eq!(restored.status().cached, 1);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("json.bak"));
    }

    #[test]
    fn cache_deepens_and_hot_memory_recedes_after_a_week() {
        let now = 2 * HOT_IDLE_MS;
        let mut state = MemoryState {
            enabled: true,
            ..Default::default()
        };
        state.facts.push(MemoryFact {
            key: "one".into(),
            text: "fact".into(),
            source_message_ids: vec![1, 2, 3],
            first_seen_ms: now - 2 * CACHE_DWELL_MS,
            last_seen_ms: now - CACHE_DWELL_MS,
            recent_mentions_ms: vec![],
            cache_since_ms: Some(now - CACHE_DWELL_MS),
            hot_since_ms: None,
        });
        assert!(rebalance(&mut state, now));
        assert!(state.facts[0].hot_since_ms.is_some());
        assert!(rebalance(&mut state, now + HOT_IDLE_MS));
        assert!(state.facts[0].hot_since_ms.is_none());
        assert!(state.facts[0].cache_since_ms.is_none());
        assert_eq!(state.facts.len(), 1);
    }
}
