pub mod s3;

use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Store bucket does not exist. {0}")]
    BucketDoesNotExist(String),
    #[error("Object does not exist. {0}")]
    DoesNotExist(String),
    #[error("Not authorized to access store. {0}")]
    NotAuthorized(String),
    #[error("Error connecting to store. {0}")]
    ConnectionError(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub timestamp: u64,
    pub size: usize,
    pub hash: u64,
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait Store: 'static {
    async fn init(&self) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn remove(&self, key: &str) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;

    // Snapshot operations
    async fn create_snapshot(&self, key: &str, timestamp: u64) -> Result<()>;
    async fn list_snapshots(&self, key: &str) -> Result<Vec<SnapshotInfo>>;
    async fn get_snapshot(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>>;
    async fn restore_from_snapshot(&self, key: &str, timestamp: u64) -> Result<()>;
    async fn delete_snapshot(&self, key: &str, timestamp: u64) -> Result<()>;
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait Store: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn remove(&self, key: &str) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;

    // Snapshot operations
    async fn create_snapshot(&self, key: &str, timestamp: u64) -> Result<()>;
    async fn list_snapshots(&self, key: &str) -> Result<Vec<SnapshotInfo>>;
    async fn get_snapshot(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>>;
    async fn restore_from_snapshot(&self, key: &str, timestamp: u64) -> Result<()>;
    async fn delete_snapshot(&self, key: &str, timestamp: u64) -> Result<()>;
}
