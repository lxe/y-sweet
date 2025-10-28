use async_trait::async_trait;
use std::{
    fs::{create_dir_all, remove_file},
    path::PathBuf,
};
use y_sweet_core::store::{Result, SnapshotInfo, Store, StoreError};

pub struct FileSystemStore {
    base_path: PathBuf,
}

impl FileSystemStore {
    pub fn new(base_path: PathBuf) -> std::result::Result<Self, std::io::Error> {
        create_dir_all(base_path.clone())?;
        Ok(Self { base_path })
    }

    fn snapshot_key(&self, base_key: &str, timestamp: u64) -> String {
        base_key.replace("/data.ysweet", &format!("/versions/version.{}.ysweet", timestamp))
    }

    fn snapshots_dir(&self, base_key: &str) -> PathBuf {
        let snapshots_prefix = base_key.replace("/data.ysweet", "/versions/");
        self.base_path.join(snapshots_prefix)
    }

    fn extract_timestamp_from_filename(&self, filename: &str) -> Option<u64> {
        filename
            .strip_prefix("version.")
            .and_then(|s| s.strip_suffix(".ysweet"))
            .and_then(|s| s.parse().ok())
    }
}

#[async_trait]
impl Store for FileSystemStore {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.base_path.join(key);
        let contents = std::fs::read(path);
        match contents {
            Ok(contents) => Ok(Some(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::ConnectionError(e.to_string())),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let path = self.base_path.join(key);
        create_dir_all(path.parent().expect("Bad parent"))
            .map_err(|_| StoreError::NotAuthorized("Error creating directories".to_string()))?;
        std::fs::write(path, value)
            .map_err(|_| StoreError::NotAuthorized("Error writing file.".to_string()))?;
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let path = self.base_path.join(key);
        remove_file(path)
            .map_err(|_| StoreError::NotAuthorized("Error removing file.".to_string()))?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.base_path.join(key);
        Ok(path.exists())
    }

    async fn create_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        // Copy current document to snapshot location
        if let Some(data) = self.get(key).await? {
            let snapshot_key = self.snapshot_key(key, timestamp);
            self.set(&snapshot_key, data).await?;
        }
        Ok(())
    }

    async fn create_snapshot_with_data(&self, key: &str, timestamp: u64, data: Vec<u8>) -> Result<()> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        self.set(&snapshot_key, data).await
    }

    async fn list_snapshots(&self, key: &str) -> Result<Vec<SnapshotInfo>> {
        let snapshots_dir = self.snapshots_dir(key);

        let mut snapshots = Vec::new();

        if !snapshots_dir.exists() {
            return Ok(snapshots);
        }

        let entries = std::fs::read_dir(&snapshots_dir)
            .map_err(|e| StoreError::ConnectionError(format!("Failed to read snapshots directory: {}", e)))?;

        for entry in entries {
            let entry = entry
                .map_err(|e| StoreError::ConnectionError(format!("Failed to read directory entry: {}", e)))?;

            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(timestamp) = self.extract_timestamp_from_filename(&filename_str) {
                let metadata = entry.metadata()
                    .map_err(|e| StoreError::ConnectionError(format!("Failed to read file metadata: {}", e)))?;

                snapshots.push(SnapshotInfo {
                    timestamp,
                    size: metadata.len() as usize,
                    hash: 0,
                });
            }
        }

        snapshots.sort_by_key(|s| s.timestamp);
        Ok(snapshots)
    }

    async fn get_snapshot(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        self.get(&snapshot_key).await
    }

    async fn restore_from_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        if let Some(snapshot_data) = self.get(&snapshot_key).await? {
            self.set(key, snapshot_data).await?;
        } else {
            return Err(StoreError::DoesNotExist(format!("Snapshot {} not found", timestamp)));
        }
        Ok(())
    }

    async fn delete_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        self.remove(&snapshot_key).await
    }
}
