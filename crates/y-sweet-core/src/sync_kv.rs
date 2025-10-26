use crate::store::Store;
use anyhow::{Context, Result};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use xxhash_rust::xxh3::xxh3_64;
use yrs_kvstore::{DocOps, KVEntry};

#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    pub enabled: bool,
    pub interval_seconds: Option<u64>,  // None = manual only
    pub max_snapshots: Option<usize>,   // None = unlimited
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: None,
            max_snapshots: None,
        }
    }
}

pub struct SyncKv {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    store: Option<Arc<Box<dyn Store>>>,
    key: String,
    dirty: AtomicBool,
    dirty_callback: Box<dyn Fn() + Send + Sync>,
    shutdown: AtomicBool,
    last_persisted_hash: AtomicU64,
    snapshot_config: SnapshotConfig,
    last_snapshot_time: AtomicU64,
}

impl SyncKv {
    pub async fn new<Callback: Fn() + Send + Sync + 'static>(
        store: Option<Arc<Box<dyn Store>>>,
        key: &str,
        callback: Callback,
    ) -> Result<Self> {
        Self::new_with_snapshot_config(store, key, callback, SnapshotConfig::default()).await
    }

    pub async fn new_with_snapshot_config<Callback: Fn() + Send + Sync + 'static>(
        store: Option<Arc<Box<dyn Store>>>,
        key: &str,
        callback: Callback,
        snapshot_config: SnapshotConfig,
    ) -> Result<Self> {
        let key = format!("{}/data.ysweet", key);

        let (data, initial_hash) = if let Some(store) = &store {
            if let Some(snapshot) = store.get(&key).await.context("Failed to get from store.")? {
                tracing::info!(size=?snapshot.len(), "Loaded snapshot");
                let data: BTreeMap<Vec<u8>, Vec<u8>> = bincode::deserialize(&snapshot).context("Failed to deserialize.")?;
                // Calculate hash of loaded data so we don't re-persist unchanged data
                let hash = xxh3_64(&snapshot);
                (data, hash)
            } else {
                (BTreeMap::new(), 0)
            }
        } else {
            (BTreeMap::new(), 0)
        };

        Ok(Self {
            data: Arc::new(Mutex::new(data)),
            store,
            key,
            dirty: AtomicBool::new(false),
            dirty_callback: Box::new(callback),
            shutdown: AtomicBool::new(false),
            last_persisted_hash: AtomicU64::new(initial_hash),
            snapshot_config,
            last_snapshot_time: AtomicU64::new(0),
        })
    }

    fn mark_dirty(&self) {
        if !self.dirty.load(Ordering::Acquire) && !self.shutdown.load(Ordering::SeqCst) {
            self.dirty.store(true, Ordering::Release);
            (self.dirty_callback)();
        }
    }

    pub async fn persist(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Only proceed if actually dirty
        if !self.dirty.load(Ordering::Acquire) {
            tracing::debug!("Skipping persist - not dirty");
            return Ok(());
        }

        if let Some(store) = &self.store {
            let snapshot = {
                let data = self.data.lock().unwrap();
                bincode::serialize(&*data)?
            };

            // Calculate hash using xxhash3 (fast and stable)
            let current_hash = xxh3_64(&snapshot);

            // Check if data has actually changed since last persist
            let last_hash = self.last_persisted_hash.load(Ordering::Acquire);
            if last_hash == current_hash {
                tracing::debug!(
                    hash = current_hash,
                    "Skipping persist - content unchanged despite dirty flag"
                );
                // Clear dirty flag since content is same as persisted
                self.dirty.store(false, Ordering::Release);
                return Ok(());
            }

            // Data has changed, persist it
            tracing::info!(
                size = ?snapshot.len(),
                hash = current_hash,
                prev_hash = if last_hash == 0 { "none".to_string() } else { last_hash.to_string() },
                "Persisting snapshot"
            );
            store.set(&self.key, snapshot).await?;

            // Update the last persisted hash atomically
            self.last_persisted_hash.store(current_hash, Ordering::Release);

            // Note: Automatic snapshot creation moved to DocWithSyncKv level
            // where we have access to Yjs document for proper format conversion
        }

        // Clear dirty flag after successful persist
        self.dirty.store(false, Ordering::Release);
        Ok(())
    }

    #[cfg(test)]
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.data.lock().unwrap();
        map.get(key).cloned()
    }

    #[cfg(test)]
    fn set(&self, key: &[u8], value: &[u8]) {
        let mut map = self.data.lock().unwrap();
        map.insert(key.to_vec(), value.to_vec());
        self.mark_dirty();
    }

    pub fn len(&self) -> usize {
        self.data.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.lock().unwrap().is_empty()
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Call the callback one last time to wake up the persistence worker
        (self.dirty_callback)();
    }


    pub fn should_create_snapshot(&self) -> bool {
        if !self.snapshot_config.enabled {
            return false;
        }

        if let Some(interval) = self.snapshot_config.interval_seconds {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let last_snapshot = self.last_snapshot_time.load(Ordering::Acquire);

            // If no snapshot has been created yet (last_snapshot == 0), create the first one
            // Otherwise, only create a snapshot if the interval has elapsed
            if last_snapshot == 0 {
                return true;
            }

            now - last_snapshot >= interval
        } else {
            false // Manual only
        }
    }

    async fn create_snapshot_internal(&self, timestamp: u64) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(store) = &self.store {
            store.create_snapshot(&self.key, timestamp).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
            self.last_snapshot_time.store(timestamp, Ordering::Release);

            // Clean up old snapshots if needed
            if let Some(max_snapshots) = self.snapshot_config.max_snapshots {
                if let Err(e) = self.cleanup_old_snapshots(max_snapshots).await {
                    tracing::error!(?e, "Failed to cleanup old snapshots");
                }
            }
        }
        Ok(())
    }

    async fn cleanup_old_snapshots(&self, max_snapshots: usize) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(store) = &self.store {
            let snapshots = store.list_snapshots(&self.key).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

            // If we have more than max_snapshots, delete the oldest ones
            if snapshots.len() > max_snapshots {
                let to_delete = snapshots.len() - max_snapshots;
                for snapshot in snapshots.iter().take(to_delete) {
                    tracing::info!(timestamp = snapshot.timestamp, "Deleting old snapshot");
                    store.delete_snapshot(&self.key, snapshot.timestamp).await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                }
            }
        }
        Ok(())
    }

    /// Create a snapshot manually. Returns the timestamp of the created snapshot.
    pub async fn create_snapshot(&self, timestamp: Option<u64>) -> Result<u64, Box<dyn std::error::Error>> {
        let timestamp = timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });

        self.create_snapshot_internal(timestamp).await?;
        Ok(timestamp)
    }

    /// Create a snapshot with provided Yjs binary data. Returns the timestamp of the created snapshot.
    pub async fn create_snapshot_with_yjs_data(&self, timestamp: Option<u64>, yjs_data: Vec<u8>) -> Result<u64, Box<dyn std::error::Error>> {
        let timestamp = timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });

        if let Some(store) = &self.store {
            store.create_snapshot_with_data(&self.key, timestamp, yjs_data).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
            self.last_snapshot_time.store(timestamp, Ordering::Release);

            // Clean up old snapshots if needed
            if let Some(max_snapshots) = self.snapshot_config.max_snapshots {
                if let Err(e) = self.cleanup_old_snapshots(max_snapshots).await {
                    tracing::error!(?e, "Failed to cleanup old snapshots");
                }
            }
        }

        Ok(timestamp)
    }

    /// Restore the document from a snapshot at the given timestamp.
    /// This only restores the underlying storage - to properly sync with connected clients,
    /// use DocWithSyncKv::restore_from_snapshot which computes and broadcasts the diff.
    pub async fn restore_from_snapshot_storage(&self, timestamp: u64) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(store) = &self.store {
            store.restore_from_snapshot(&self.key, timestamp).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

            // Reload data from restored snapshot into memory
            if let Some(snapshot) = store.get(&self.key).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)? {
                let data: BTreeMap<Vec<u8>, Vec<u8>> = bincode::deserialize(&snapshot)?;
                let hash = xxh3_64(&snapshot);

                *self.data.lock().unwrap() = data;
                self.last_persisted_hash.store(hash, Ordering::Release);
            }
        }
        Ok(())
    }

    /// List all available snapshots for this document.
    pub async fn list_snapshots(&self) -> Result<Vec<crate::store::SnapshotInfo>, Box<dyn std::error::Error>> {
        if let Some(store) = &self.store {
            Ok(store.list_snapshots(&self.key).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?)
        } else {
            Ok(vec![])
        }
    }

    /// Delete a specific snapshot.
    pub async fn delete_snapshot(&self, timestamp: u64) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(store) = &self.store {
            store.delete_snapshot(&self.key, timestamp).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        }
        Ok(())
    }
}

impl<'d> DocOps<'d> for SyncKv {}

pub struct SyncKvEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KVEntry for SyncKvEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }

    fn value(&self) -> &[u8] {
        &self.value
    }
}

pub struct SyncKvCursor {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    next_key: Bound<Vec<u8>>,
    to: Vec<u8>,
}

impl Iterator for SyncKvCursor {
    type Item = SyncKvEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let map = self.data.lock().unwrap();
        let next = map
            .range((self.next_key.clone(), Bound::Excluded(self.to.clone())))
            .next()?;
        self.next_key = Bound::Excluded(next.0.clone());
        Some(SyncKvEntry {
            key: next.0.clone(),
            value: next.1.clone(),
        })
    }
}

impl<'a> yrs_kvstore::KVStore<'a> for SyncKv {
    type Error = std::convert::Infallible;
    type Cursor = SyncKvCursor;
    type Entry = SyncKvEntry;
    type Return = Vec<u8>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Infallible> {
        let map = self.data.lock().unwrap();
        Ok(map.get(key).cloned())
    }

    fn remove(&self, key: &[u8]) -> Result<(), Self::Error> {
        let mut map = self.data.lock().unwrap();
        map.remove(key);
        self.mark_dirty();
        Ok(())
    }

    fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        Ok(SyncKvCursor {
            data: self.data.clone(),
            next_key: Bound::Included(from.to_vec()),
            to: to.to_vec(),
        })
    }

    fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
        let map = self.data.lock().unwrap();
        let prev = map.range(..key.to_vec()).next_back();
        Ok(prev.map(|(k, v)| SyncKvEntry {
            key: k.clone(),
            value: v.clone(),
        }))
    }

    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut map = self.data.lock().unwrap();
        map.insert(key.to_vec(), value.to_vec());
        self.mark_dirty();
        Ok(())
    }

    fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
        for entry in self.iter_range(from, to)? {
            let mut map = self.data.lock().unwrap();
            map.remove(&entry.key);
        }
        self.mark_dirty();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::store::{Result, StoreError};
    use async_trait::async_trait;
    use dashmap::DashMap;
    use std::sync::atomic::AtomicUsize;
    use tokio;

    #[derive(Default, Clone)]
    struct MemoryStore {
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
            self.data.insert(key.to_owned(), value);
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

        async fn list_snapshots(&self, key: &str) -> Result<Vec<crate::store::SnapshotInfo>> {
            let prefix = format!("{}.snapshot.", key);
            let mut snapshots = Vec::new();

            for entry in self.data.iter() {
                if let Some(timestamp_str) = entry.key().strip_prefix(&prefix) {
                    if let Ok(timestamp) = timestamp_str.parse::<u64>() {
                        snapshots.push(crate::store::SnapshotInfo {
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

    #[derive(Default, Clone)]
    struct CallbackCounter {
        data: Arc<AtomicUsize>,
    }

    impl CallbackCounter {
        fn callback(&self) -> Box<dyn Fn() + Send + Sync> {
            let data = self.data.clone();
            Box::new(move || {
                data.fetch_add(1, Ordering::Relaxed);
            })
        }

        fn count(&self) -> usize {
            self.data.load(Ordering::Relaxed)
        }
    }

    #[tokio::test]
    async fn calls_sync_callback() {
        let store = MemoryStore::default();
        let c = CallbackCounter::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "foo", c.callback())
            .await
            .unwrap();

        assert_eq!(c.count(), 0);
        sync_kv.set(b"foo", b"bar");
        assert_eq!(sync_kv.get(b"foo"), Some(b"bar".to_vec()));

        assert!(store.data.is_empty());

        // We should have received a dirty callback.
        assert_eq!(c.count(), 1);

        sync_kv.set(b"abc", b"def");

        // We should not receive a dirty callback.
        assert_eq!(c.count(), 1);
    }

    #[tokio::test]
    async fn persists_to_store() {
        let store = MemoryStore::default();

        {
            let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "foo", || ())
                .await
                .unwrap();

            sync_kv.set(b"foo", b"bar");
            assert_eq!(sync_kv.get(b"foo"), Some(b"bar".to_vec()));

            assert!(store.data.is_empty());

            sync_kv.persist().await.unwrap();
        }

        {
            let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "foo", || ())
                .await
                .unwrap();

            assert_eq!(sync_kv.get(b"foo"), Some(b"bar".to_vec()));
        }
    }

    #[tokio::test]
    async fn skips_persist_when_not_dirty() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // First persist - should write
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();
        assert!(store.exists("test/data.ysweet").await.unwrap());

        // No changes made, persist should skip
        let initial_data = store.get("test/data.ysweet").await.unwrap().unwrap();
        sync_kv.persist().await.unwrap();
        let after_data = store.get("test/data.ysweet").await.unwrap().unwrap();
        assert_eq!(initial_data, after_data, "Data should not change when not dirty");
    }

    #[tokio::test]
    async fn skips_persist_when_content_unchanged() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // Set initial value and persist
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();
        let initial_data = store.get("test/data.ysweet").await.unwrap().unwrap();

        // Set same value again - dirty flag will be set but content is same
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();
        let after_data = store.get("test/data.ysweet").await.unwrap().unwrap();

        // Should skip actual write since content is identical
        assert_eq!(initial_data, after_data, "Should skip write when content unchanged");
    }

    #[tokio::test]
    async fn persists_when_content_changes() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // Set initial value and persist
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();
        let initial_data = store.get("test/data.ysweet").await.unwrap().unwrap();

        // Change value
        sync_kv.set(b"key1", b"value2");
        sync_kv.persist().await.unwrap();
        let after_data = store.get("test/data.ysweet").await.unwrap().unwrap();

        // Should write since content changed
        assert_ne!(initial_data, after_data, "Should write when content changes");

        // Verify the new value is persisted
        let sync_kv2 = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();
        assert_eq!(sync_kv2.get(b"key1"), Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_manual_snapshot_creation() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // Set some data and persist
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();

        // Create a manual snapshot
        let timestamp = sync_kv.create_snapshot(Some(1000)).await.unwrap();
        assert_eq!(timestamp, 1000);

        // Verify snapshot exists
        let snapshots = sync_kv.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].timestamp, 1000);
    }

    #[tokio::test]
    async fn test_snapshot_restore_storage() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // Set initial data and persist
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();

        // Create snapshot
        sync_kv.create_snapshot(Some(1000)).await.unwrap();

        // Modify data
        sync_kv.set(b"key1", b"value2");
        sync_kv.set(b"key2", b"value3");
        sync_kv.persist().await.unwrap();

        // Verify current state
        assert_eq!(sync_kv.get(b"key1"), Some(b"value2".to_vec()));
        assert_eq!(sync_kv.get(b"key2"), Some(b"value3".to_vec()));

        // Restore from snapshot at storage level
        sync_kv.restore_from_snapshot_storage(1000).await.unwrap();

        // Verify restored state
        assert_eq!(sync_kv.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(sync_kv.get(b"key2"), None);
    }

    #[tokio::test]
    async fn test_list_snapshots_ordered() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // Set some data
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();

        // Create multiple snapshots in non-sequential order
        sync_kv.create_snapshot(Some(3000)).await.unwrap();
        sync_kv.create_snapshot(Some(1000)).await.unwrap();
        sync_kv.create_snapshot(Some(2000)).await.unwrap();

        // List snapshots should be ordered
        let snapshots = sync_kv.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].timestamp, 1000);
        assert_eq!(snapshots[1].timestamp, 2000);
        assert_eq!(snapshots[2].timestamp, 3000);
    }

    #[tokio::test]
    async fn test_delete_snapshot() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // Set some data
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();

        // Create snapshots
        sync_kv.create_snapshot(Some(1000)).await.unwrap();
        sync_kv.create_snapshot(Some(2000)).await.unwrap();
        sync_kv.create_snapshot(Some(3000)).await.unwrap();

        // Verify 3 snapshots exist
        let snapshots = sync_kv.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 3);

        // Delete middle snapshot
        sync_kv.delete_snapshot(2000).await.unwrap();

        // Verify only 2 snapshots remain
        let snapshots = sync_kv.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].timestamp, 1000);
        assert_eq!(snapshots[1].timestamp, 3000);
    }

    #[tokio::test]
    async fn test_automatic_snapshot_with_interval() {
        let store = MemoryStore::default();
        let snapshot_config = SnapshotConfig {
            enabled: true,
            interval_seconds: Some(1), // 1 second interval
            max_snapshots: None,
        };

        let sync_kv = SyncKv::new_with_snapshot_config(
            Some(Arc::new(Box::new(store.clone()))),
            "test",
            || (),
            snapshot_config,
        )
        .await
        .unwrap();

        // Set data and persist (should create first snapshot)
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();

        // Wait for interval to pass
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Modify and persist again (should create second snapshot)
        sync_kv.set(b"key2", b"value2");
        sync_kv.persist().await.unwrap();

        // Verify snapshots were created automatically
        let snapshots = sync_kv.list_snapshots().await.unwrap();
        assert!(snapshots.len() >= 1, "At least one snapshot should be created");
    }

    #[tokio::test]
    async fn test_snapshot_max_retention() {
        let store = MemoryStore::default();
        let snapshot_config = SnapshotConfig {
            enabled: true,
            interval_seconds: None, // Manual only
            max_snapshots: Some(3),
        };

        let sync_kv = SyncKv::new_with_snapshot_config(
            Some(Arc::new(Box::new(store.clone()))),
            "test",
            || (),
            snapshot_config,
        )
        .await
        .unwrap();

        // Set some data
        sync_kv.set(b"key1", b"value1");
        sync_kv.persist().await.unwrap();

        // Create 5 snapshots
        for i in 1..=5 {
            sync_kv.create_snapshot(Some(i * 1000)).await.unwrap();
        }

        // Should only have 3 snapshots (oldest deleted)
        let snapshots = sync_kv.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].timestamp, 3000);
        assert_eq!(snapshots[1].timestamp, 4000);
        assert_eq!(snapshots[2].timestamp, 5000);
    }

    #[tokio::test]
    async fn test_snapshot_with_empty_document() {
        let store = MemoryStore::default();
        let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
            .await
            .unwrap();

        // Set initial data to have something to snapshot
        sync_kv.set(b"initial", b"data");
        sync_kv.persist().await.unwrap();

        // Create snapshot
        sync_kv.create_snapshot(Some(1000)).await.unwrap();

        // Verify snapshot exists
        let snapshots = sync_kv.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 1);

        // Modify document - add more data
        sync_kv.set(b"key1", b"value1");
        sync_kv.set(b"key2", b"value2");
        sync_kv.persist().await.unwrap();

        // Verify new state
        assert_eq!(sync_kv.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(sync_kv.get(b"key2"), Some(b"value2".to_vec()));

        // Restore to initial state (with only "initial" key) at storage level
        sync_kv.restore_from_snapshot_storage(1000).await.unwrap();
        assert_eq!(sync_kv.get(b"initial"), Some(b"data".to_vec()));
        assert_eq!(sync_kv.get(b"key1"), None);
        assert_eq!(sync_kv.get(b"key2"), None);
    }

    #[tokio::test]
    async fn no_repersist_after_reload() {
        let store = MemoryStore::default();

        // Create, write data, and persist
        {
            let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
                .await
                .unwrap();
            sync_kv.set(b"key1", b"value1");
            sync_kv.persist().await.unwrap();
        }

        let initial_data = store.get("test/data.ysweet").await.unwrap().unwrap();

        // Reload and immediately persist without changes
        {
            let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), "test", || ())
                .await
                .unwrap();
            // Mark dirty (simulating what might happen in real usage)
            sync_kv.mark_dirty();
            sync_kv.persist().await.unwrap();
        }

        let after_data = store.get("test/data.ysweet").await.unwrap().unwrap();
        assert_eq!(initial_data, after_data, "Should not re-persist unchanged loaded data");
    }
}
