//! Process-local image attachments shared by chat, Codex, and the web server.

use std::{
    collections::HashMap,
    io::Cursor,
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use image::{ImageDecoder, ImageFormat, codecs::gif::GifDecoder};
use serde::{Deserialize, Serialize};

pub const MAX_MEDIA_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_MESSAGE_MEDIA: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MediaAsset {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    pub path: PathBuf,
    pub model_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MediaView {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    pub url: String,
}

impl MediaAsset {
    #[must_use]
    pub fn view(&self) -> MediaView {
        MediaView {
            id: self.id.clone(),
            name: self.name.clone(),
            mime_type: self.mime_type.clone(),
            url: format!("/api/media/{}", self.id),
        }
    }
}

#[derive(Debug)]
pub struct MediaStore {
    root: PathBuf,
    next_id: AtomicU64,
    assets: Mutex<HashMap<String, MediaAsset>>,
}

impl MediaStore {
    pub fn create() -> Result<Self, std::io::Error> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("norma-media-{}-{stamp}", std::process::id()));
        Self::open(root)
    }

    pub fn open(root: PathBuf) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            next_id: AtomicU64::new(1),
            assets: Mutex::new(HashMap::new()),
        })
    }

    pub fn restore(&self, assets: Vec<MediaAsset>) -> Result<(), std::io::Error> {
        let mut registered = self.assets.lock().expect("media mutex");
        for asset in assets {
            if !asset.path.is_file() || asset.model_paths.iter().any(|path| !path.is_file()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("stored media file is missing: {}", asset.id),
                ));
            }
            if let Some(counter) = asset
                .id
                .strip_prefix(&format!("{}-", std::process::id()))
                .and_then(|suffix| suffix.parse::<u64>().ok())
            {
                self.next_id.fetch_max(counter + 1, Ordering::Relaxed);
            }
            registered.insert(asset.id.clone(), asset);
        }
        Ok(())
    }

    pub fn save(&self, filename: &str, bytes: &[u8]) -> Result<MediaAsset, String> {
        if bytes.is_empty() || bytes.len() > MAX_MEDIA_BYTES {
            return Err(format!(
                "image must be 1 byte to {} MiB",
                MAX_MEDIA_BYTES / 1024 / 1024
            ));
        }
        let (mime, extension) =
            image_kind(bytes).ok_or("only PNG, JPEG, WebP, and GIF images are supported")?;
        let format = match mime {
            "image/png" => ImageFormat::Png,
            "image/jpeg" => ImageFormat::Jpeg,
            "image/webp" => ImageFormat::WebP,
            _ => ImageFormat::Gif,
        };
        let (width, height) = image::ImageReader::with_format(Cursor::new(bytes), format)
            .into_dimensions()
            .map_err(|error| format!("invalid image: {error}"))?;
        if width > 4096 || height > 4096 || width == 0 || height == 0 {
            return Err("image dimensions must be 1–4096 pixels".into());
        }
        let name = safe_name(filename, extension);
        let id = format!(
            "{}-{}",
            std::process::id(),
            self.next_id.fetch_add(1, Ordering::Relaxed)
        );
        let path = self.root.join(format!("{id}.{extension}"));
        std::fs::write(&path, bytes).map_err(|error| error.to_string())?;
        let model_paths = if mime == "image/gif" {
            let decoded = (|| {
                let decoder =
                    GifDecoder::new(Cursor::new(bytes)).map_err(|error| error.to_string())?;
                let _ = decoder.dimensions();
                let preview = image::load_from_memory_with_format(bytes, ImageFormat::Gif)
                    .map_err(|error| error.to_string())?;
                let preview_path = self.root.join(format!("{id}-first-frame.png"));
                preview
                    .save_with_format(&preview_path, ImageFormat::Png)
                    .map_err(|error| error.to_string())?;
                Ok(vec![preview_path])
            })();
            match decoded {
                Ok(paths) => paths,
                Err(error) => {
                    let _ = std::fs::remove_file(&path);
                    return Err(error);
                }
            }
        } else {
            vec![path.clone()]
        };
        let asset = MediaAsset {
            id: id.clone(),
            name,
            mime_type: mime.into(),
            path,
            model_paths,
        };
        self.assets
            .lock()
            .expect("media mutex")
            .insert(id, asset.clone());
        Ok(asset)
    }

    pub fn save_base64(
        &self,
        filename: &str,
        mime_type: &str,
        encoded: &str,
    ) -> Result<MediaAsset, String> {
        if encoded.len() > (MAX_MEDIA_BYTES * 4 / 3 + 16) {
            return Err("image is too large".into());
        }
        let bytes = STANDARD
            .decode(encoded)
            .map_err(|_| "invalid base64 image".to_owned())?;
        let actual = image_kind(&bytes).ok_or("unsupported image format")?.0;
        if actual != mime_type {
            return Err("image MIME type does not match its bytes".into());
        }
        self.save(filename, &bytes)
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<MediaAsset> {
        self.assets.lock().expect("media mutex").get(id).cloned()
    }

    pub fn remove(&self, id: &str) {
        if let Some(asset) = self.assets.lock().expect("media mutex").remove(id) {
            for path in asset.model_paths.iter().chain(std::iter::once(&asset.path)) {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

fn safe_name(filename: &str, extension: &str) -> String {
    let leaf = filename.rsplit(['/', '\\']).next().unwrap_or("image");
    let clean = leaf
        .chars()
        .filter(|ch| !ch.is_control())
        .take(120)
        .collect::<String>();
    if clean.trim().is_empty() {
        format!("image.{extension}")
    } else {
        clean
    }
}

fn image_kind(bytes: &[u8]) -> Option<(&'static str, &'static str)> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some(("image/png", "png"))
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some(("image/jpeg", "jpg"))
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some(("image/gif", "gif"))
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some(("image/webp", "webp"))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn png_and_gif_keep_original_and_model_readable_frame() {
        let store = MediaStore::create().unwrap();
        let image = image::DynamicImage::new_rgba8(2, 2);
        for format in [ImageFormat::Png, ImageFormat::Gif] {
            let mut bytes = Cursor::new(Vec::new());
            image.write_to(&mut bytes, format).unwrap();
            let filename = if format == ImageFormat::Gif {
                "motion.gif"
            } else {
                "still.png"
            };
            let asset = store.save(filename, &bytes.into_inner()).unwrap();
            assert_eq!(asset.name, filename);
            assert!(asset.path.is_file());
            assert!(asset.model_paths[0].is_file());
            if format == ImageFormat::Gif {
                assert!(
                    asset.model_paths[0]
                        .to_string_lossy()
                        .ends_with("first-frame.png")
                );
            } else {
                assert_eq!(asset.path, asset.model_paths[0]);
            }
            assert_eq!(store.get(&asset.id), Some(asset.clone()));
            store.remove(&asset.id);
            assert!(!asset.path.exists());
        }
    }

    #[test]
    fn rejects_non_images_and_mismatched_mime() {
        let store = MediaStore::create().unwrap();
        assert!(store.save("text.png", b"not an image").is_err());
        let mut bytes = Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(1, 1)
            .write_to(&mut bytes, ImageFormat::Png)
            .unwrap();
        let encoded = STANDARD.encode(bytes.into_inner());
        assert!(
            store
                .save_base64("image.gif", "image/gif", &encoded)
                .is_err()
        );
    }
}
