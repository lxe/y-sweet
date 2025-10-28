use crate::{doc_connection::DOC_NAME, store::Store, sync::awareness::Awareness, sync_kv::{SyncKv, SnapshotConfig}};
use anyhow::{anyhow, Context, Result};
use std::sync::{Arc, RwLock};
use yrs::{updates::decoder::Decode, Doc, ReadTxn, StateVector, Subscription, Transact, Update};
use yrs_kvstore::DocOps;

pub struct DocWithSyncKv {
    awareness: Arc<RwLock<Awareness>>,
    sync_kv: Arc<SyncKv>,
    #[allow(unused)] // acts as RAII guard
    subscription: Subscription,
}

impl DocWithSyncKv {
    pub fn awareness(&self) -> Arc<RwLock<Awareness>> {
        self.awareness.clone()
    }

    pub fn sync_kv(&self) -> Arc<SyncKv> {
        self.sync_kv.clone()
    }

    pub async fn new<F>(
        key: &str,
        store: Option<Arc<Box<dyn Store>>>,
        dirty_callback: F,
    ) -> Result<Self>
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self::new_with_snapshot_config(key, store, dirty_callback, SnapshotConfig::default()).await
    }

    pub async fn new_with_snapshot_config<F>(
        key: &str,
        store: Option<Arc<Box<dyn Store>>>,
        dirty_callback: F,
        snapshot_config: SnapshotConfig,
    ) -> Result<Self>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let sync_kv = SyncKv::new_with_snapshot_config(store, key, dirty_callback, snapshot_config)
            .await
            .context("Failed to create SyncKv")?;

        let sync_kv = Arc::new(sync_kv);
        let doc = Doc::new();

        {
            let mut txn = doc.transact_mut();
            sync_kv
                .load_doc(DOC_NAME, &mut txn)
                .map_err(|_| anyhow!("Failed to load doc"))?;
        }

        let subscription = {
            let sync_kv = sync_kv.clone();
            doc.observe_update_v1(move |_, event| {
                sync_kv.push_update(DOC_NAME, &event.update).unwrap();
                sync_kv
                    .flush_doc_with(DOC_NAME, Default::default())
                    .unwrap();
            })
            .map_err(|_| anyhow!("Failed to subscribe to updates"))?
        };

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        Ok(Self {
            awareness,
            sync_kv,
            subscription,
        })
    }

    pub fn as_update(&self) -> Vec<u8> {
        let awareness_guard = self.awareness.read().unwrap();
        let doc = &awareness_guard.doc;

        let txn = doc.transact();

        txn.encode_state_as_update_v1(&StateVector::default())
    }

    pub fn apply_update(&self, update: &[u8]) -> Result<()> {
        let awareness_guard = self.awareness.write().unwrap();
        let doc = &awareness_guard.doc;

        let update: Update =
            Update::decode_v1(update).map_err(|_| anyhow!("Failed to decode update"))?;

        let mut txn = doc.transact_mut();
        txn.apply_update(update);

        Ok(())
    }

    /// Create a snapshot with proper Yjs binary format.
    /// This creates a snapshot that can be directly applied with Y.applyUpdate() on the client.
    pub async fn create_snapshot(&self, timestamp: Option<u64>) -> Result<u64> {
        let timestamp = timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });

        // Get the document as a Yjs binary update
        let yjs_data = self.as_update();

        // Use the SyncKv's create_snapshot method but pass the Yjs data
        self.sync_kv.create_snapshot_with_yjs_data(Some(timestamp), yjs_data).await
            .map_err(|e| anyhow!("Failed to create snapshot: {}", e))?;

        Ok(timestamp)
    }

    /// Check if an automatic snapshot should be created and create it if needed.
    /// This should be called after document persistence.
    pub async fn check_and_create_automatic_snapshot(&self) -> Result<()> {
        if self.sync_kv.should_create_snapshot() {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if let Err(e) = self.create_snapshot(Some(timestamp)).await {
                tracing::error!(?e, "Failed to create automatic snapshot");
            }
        }
        Ok(())
    }

    /// Restore the document from a snapshot at the given timestamp.
    /// This restores the snapshot and **disconnects all clients**, requiring them to reconnect
    /// to receive the restored state. Yjs CRDTs merge updates rather than replacing state,
    /// so a full document replacement is necessary for proper snapshot restoration.
    pub async fn restore_from_snapshot(&self, timestamp: u64) -> Result<()> {
        // Restore the snapshot in storage
        self.sync_kv.restore_from_snapshot_storage(timestamp).await
            .map_err(|e| anyhow!("Failed to restore snapshot storage: {}", e))?;

        // Note: After restoration, clients should reconnect to receive the restored state.
        // The server implementation should close WebSocket connections for this document
        // after calling this method to force clients to reconnect and sync the restored state.

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Result as StoreResult, StoreError, SnapshotInfo};
    use async_trait::async_trait;
    use dashmap::DashMap;
    use std::sync::Arc;
    use yrs::{Map, Transact, WriteTxn};

    #[derive(Default, Clone)]
    struct MemoryStore {
        data: Arc<DashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl Store for MemoryStore {
        async fn init(&self) -> StoreResult<()> {
            Ok(())
        }

        async fn get(&self, key: &str) -> StoreResult<Option<Vec<u8>>> {
            Ok(self.data.get(key).map(|v| v.clone()))
        }

        async fn set(&self, key: &str, value: Vec<u8>) -> StoreResult<()> {
            self.data.insert(key.to_string(), value);
            Ok(())
        }

        async fn remove(&self, key: &str) -> StoreResult<()> {
            self.data.remove(key);
            Ok(())
        }

        async fn exists(&self, key: &str) -> StoreResult<bool> {
            Ok(self.data.contains_key(key))
        }

        async fn create_snapshot(&self, key: &str, timestamp: u64) -> StoreResult<()> {
            if let Some(data) = self.get(key).await? {
                let snapshot_key = format!("{}.version.{}", key, timestamp);
                self.set(&snapshot_key, data).await?;
            }
            Ok(())
        }

        async fn create_snapshot_with_data(&self, key: &str, timestamp: u64, data: Vec<u8>) -> StoreResult<()> {
            let snapshot_key = format!("{}.version.{}", key, timestamp);
            self.set(&snapshot_key, data).await
        }

        async fn list_snapshots(&self, key: &str) -> StoreResult<Vec<SnapshotInfo>> {
            let prefix = format!("{}.version.", key);
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

        async fn get_snapshot(&self, key: &str, timestamp: u64) -> StoreResult<Option<Vec<u8>>> {
            let snapshot_key = format!("{}.version.{}", key, timestamp);
            self.get(&snapshot_key).await
        }

        async fn restore_from_snapshot(&self, key: &str, timestamp: u64) -> StoreResult<()> {
            let snapshot_key = format!("{}.version.{}", key, timestamp);
            if let Some(snapshot_data) = self.get(&snapshot_key).await? {
                self.set(key, snapshot_data).await?;
            } else {
                return Err(StoreError::DoesNotExist(format!("Snapshot {} not found", timestamp)));
            }
            Ok(())
        }

        async fn delete_snapshot(&self, key: &str, timestamp: u64) -> StoreResult<()> {
            let snapshot_key = format!("{}.version.{}", key, timestamp);
            self.remove(&snapshot_key).await
        }
    }

    #[tokio::test]
    async fn test_snapshot_restore_and_reload() {
        let store = MemoryStore::default();
        let doc_with_kv = DocWithSyncKv::new("test", Some(Arc::new(Box::new(store.clone()))), || ())
            .await
            .unwrap();

        // Set initial data using Yjs
        {
            let awareness = doc_with_kv.awareness();
            let awareness_guard = awareness.read().unwrap();
            let mut txn = awareness_guard.doc.transact_mut();
            let map = txn.get_or_insert_map("data");
            map.insert(&mut txn, "key1", "value1");
        }

        // Persist and create snapshot
        doc_with_kv.sync_kv().persist().await.unwrap();
        doc_with_kv.sync_kv().create_snapshot(Some(1000)).await.unwrap();

        // Modify data
        {
            let awareness = doc_with_kv.awareness();
            let awareness_guard = awareness.read().unwrap();
            let mut txn = awareness_guard.doc.transact_mut();
            let map = txn.get_or_insert_map("data");
            map.insert(&mut txn, "key1", "value2");
            map.insert(&mut txn, "key2", "value3");
        }

        // Persist changes
        doc_with_kv.sync_kv().persist().await.unwrap();

        // Verify current state
        {
            let awareness = doc_with_kv.awareness();
            let awareness_guard = awareness.read().unwrap();
            let txn = awareness_guard.doc.transact();
            let map = txn.get_map("data").unwrap();
            assert_eq!(map.get(&txn, "key1").unwrap().to_string(&txn), "value2");
            assert_eq!(map.get(&txn, "key2").unwrap().to_string(&txn), "value3");
        }

        // Restore from snapshot
        doc_with_kv.restore_from_snapshot(1000).await.unwrap();

        // In a real scenario, clients would be disconnected here and would reconnect.
        // Simulate this by creating a new DocWithSyncKv instance
        drop(doc_with_kv);
        let reloaded_doc = DocWithSyncKv::new("test", Some(Arc::new(Box::new(store.clone()))), || ())
            .await
            .unwrap();

        // Verify restored state after reconnection
        {
            let awareness = reloaded_doc.awareness();
            let awareness_guard = awareness.read().unwrap();
            let txn = awareness_guard.doc.transact();
            let map = txn.get_map("data").unwrap();
            assert_eq!(map.get(&txn, "key1").unwrap().to_string(&txn), "value1");
            assert!(map.get(&txn, "key2").is_none());
        }
    }
}
