use std::path::PathBuf;

use sha2::{Digest, Sha256};

/// Filesystem-backed key/value cache mirroring Python's `KVCache`.
///
/// Values are stored as UTF-8 text files at:
///   `~/.tensorlake/cache/{namespace}/{sha256(key)}.txt`
pub struct KvCache {
    ns_dir: PathBuf,
}

impl KvCache {
    pub fn new(namespace: &str) -> Self {
        let root = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".tensorlake")
            .join("cache");
        Self {
            ns_dir: root.join(namespace),
        }
    }

    fn key_path(&self, key: &str) -> PathBuf {
        let digest = Sha256::digest(key.as_bytes());
        self.ns_dir.join(format!("{}.txt", hex::encode(digest)))
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        tokio::fs::read_to_string(self.key_path(key)).await.ok()
    }

    pub async fn set(&self, key: &str, value: &str) {
        let path = self.key_path(key);
        let _ = tokio::fs::create_dir_all(&self.ns_dir).await;
        let _ = tokio::fs::write(path, value.as_bytes()).await;
    }
}
