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

pub struct SyncKv {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    store: Option<Arc<Box<dyn Store>>>,
    key: String,
    dirty: AtomicBool,
    dirty_callback: Box<dyn Fn() + Send + Sync>,
    shutdown: AtomicBool,
    last_persisted_hash: AtomicU64,
}

impl SyncKv {
    pub async fn new<Callback: Fn() + Send + Sync + 'static>(
        store: Option<Arc<Box<dyn Store>>>,
        key: &str,
        callback: Callback,
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
    use crate::store::Result;
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
