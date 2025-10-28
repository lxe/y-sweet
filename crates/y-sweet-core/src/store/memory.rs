use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use crate::store::{Result, SnapshotInfo, Store, StoreError};


#[derive(Default, Clone)]
pub struct MemoryStore {
    data: Arc<DashMap<String, Vec<u8>>>,
}

#[cfg_attr(not(feature = "single-threaded"), async_trait)]
#[cfg_attr(feature = "single-threaded", async_trait(?Send))]
impl Store for MemoryStore {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).map(|v| v.clone()))
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_string(), value);
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.data.contains_key(key))
    }

    async fn create_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        if let Some(data) = self.get(key).await? {
            let snapshot_key = format!("{}.snapshot.{}", key, timestamp);
            self.set(&snapshot_key, data).await?;
        }
        Ok(())
    }

    async fn list_snapshots(&self, key: &str) -> Result<Vec<SnapshotInfo>> {
        let prefix = format!("{}.snapshot.", key);
        let mut snapshots = Vec::new();

        for entry in self.data.iter() {
            if let Some(timestamp_str) = entry.key().strip_prefix(&prefix) {
                if let Ok(timestamp) = timestamp_str.parse::<u64>() {
                    snapshots.push(SnapshotInfo {
                        timestamp,
                        size: entry.value().len(),
                        hash: 0,
                    });
                }
            }
        }

        snapshots.sort_by_key(|s| s.timestamp);
        Ok(snapshots)
    }

    async fn get_snapshot(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>> {
        let snapshot_key = format!("{}.snapshot.{}", key, timestamp);
        self.get(&snapshot_key).await
    }

    async fn restore_from_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        let snapshot_key = format!("{}.snapshot.{}", key, timestamp);
        if let Some(snapshot_data) = self.get(&snapshot_key).await? {
            self.set(key, snapshot_data).await?;
        } else {
            return Err(StoreError::DoesNotExist(format!("Snapshot {} not found", timestamp)));
        }
        Ok(())
    }

    async fn delete_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        let snapshot_key = format!("{}.snapshot.{}", key, timestamp);
        self.remove(&snapshot_key).await
    }
}


impl MemoryStore {
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.data.is_empty())
    }
}