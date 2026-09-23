//! Durable snapshots of chat rooms and their media references.

use std::{collections::HashMap, fs, io, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::media::MediaAsset;

use super::{ChatRoom, RoomState};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ChatSnapshot {
    pub rooms: Vec<ChatRoom>,
    pub media: Vec<MediaAsset>,
}

#[derive(Debug)]
pub struct ChatStorage {
    path: PathBuf,
}

impl ChatStorage {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self { path })
    }

    pub fn load(&self) -> io::Result<Option<ChatSnapshot>> {
        let backup = self.path.with_extension("json.bak");
        for candidate in [&self.path, &backup] {
            match fs::read(candidate) {
                Ok(bytes) => match serde_json::from_slice(&bytes) {
                    Ok(snapshot) => return Ok(Some(snapshot)),
                    Err(error) if candidate == &self.path && backup.exists() => {
                        eprintln!("chat snapshot is invalid ({error}); trying backup");
                    }
                    Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
                },
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    pub(super) fn save(
        &self,
        rooms: &std::collections::BTreeMap<u64, RoomState>,
    ) -> io::Result<()> {
        let mut media = HashMap::new();
        let rooms = rooms
            .values()
            .filter(|state| !state.room.incognito)
            .map(|state| {
                for message in &state.room.messages {
                    for asset in &message.media {
                        media.insert(asset.id.clone(), asset.clone());
                    }
                }
                state.room.clone()
            })
            .collect();
        let snapshot = ChatSnapshot {
            rooms,
            media: media.into_values().collect(),
        };
        let encoded = serde_json::to_vec(&snapshot)?;
        let pending = self.path.with_extension("json.tmp");
        let backup = self.path.with_extension("json.bak");
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
