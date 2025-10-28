use super::{Result, StoreError, SnapshotInfo};
use crate::store::Store;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::OnceLock;
use tracing;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3Config {
    pub key: String,
    pub endpoint: String,
    pub secret: String,
    pub token: Option<String>,
    pub bucket: String,
    pub region: String,
    pub bucket_prefix: Option<String>,

    // Use old path-style URLs, needed to support some S3-compatible APIs (including some minio setups)
    pub path_style: bool,
}


pub struct S3Store {
    client: OnceLock<Client>,
    config: S3Config,
    _bucket_checked: OnceLock<()>,
}

impl S3Store {
    pub fn new(config: S3Config) -> Self {
        // Ensure SSL_CERT_FILE is set to help rustls find native certificates
        Self::ensure_ssl_cert_file_set();

        S3Store {
            client: OnceLock::new(),
            config,
            _bucket_checked: OnceLock::new(),
        }
    }

    /// Ensure SSL_CERT_FILE environment variable is set to help rustls find CA certificates
    fn ensure_ssl_cert_file_set() {
        if env::var("SSL_CERT_FILE").is_err() {
            // Try common locations for CA certificates in Docker containers
            let cert_paths = [
                "/etc/ssl/certs/ca-certificates.crt",  // Debian/Ubuntu
                "/etc/pki/tls/certs/ca-bundle.crt",    // RedHat/CentOS
                "/etc/ssl/ca-bundle.pem",               // OpenSUSE
                "/etc/ssl/cert.pem",                    // Alpine
            ];

            for cert_path in &cert_paths {
                if std::path::Path::new(cert_path).exists() {
                    env::set_var("SSL_CERT_FILE", cert_path);
                    tracing::info!("Set SSL_CERT_FILE to {}", cert_path);
                    break;
                }
            }
        }
    }

    async fn get_client(&self) -> Result<&Client> {
        if let Some(client) = self.client.get() {
            return Ok(client);
        }

        let mut aws_config_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(self.config.region.clone()))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                self.config.key.clone(),
                self.config.secret.clone(),
                self.config.token.clone(),
                None,
                "y-sweet",
            ))
            // Configure timeouts for better connection reuse
            .timeout_config(
                aws_config::timeout::TimeoutConfig::builder()
                    .connect_timeout(std::time::Duration::from_secs(3))
                    .operation_timeout(std::time::Duration::from_secs(60))
                    .build()
            )
            // Enable stalled stream protection
            .stalled_stream_protection(
                aws_config::stalled_stream_protection::StalledStreamProtectionConfig::enabled()
                    .grace_period(std::time::Duration::from_secs(5))
                    .build()
            )
            // Retry configuration for resilience
            .retry_config(
                aws_config::retry::RetryConfig::standard()
                    .with_max_attempts(3)
            );

        if !self.config.endpoint.is_empty() && self.config.endpoint != "https://s3.amazonaws.com" {
            aws_config_builder = aws_config_builder.endpoint_url(&self.config.endpoint);
        }

        let aws_config = aws_config_builder.load().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        if self.config.path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        match self.client.set(client) {
            Ok(()) => Ok(self.client.get().unwrap()),
            Err(_) => Ok(self.client.get().unwrap()), // Another thread set it first
        }
    }

    /// Helper function to handle AWS SDK errors and convert them to StoreError
    fn handle_aws_error<E>(error: SdkError<E>, operation: &str, bucket: &str, key: Option<&str>) -> StoreError
    where
        E: std::fmt::Debug + std::fmt::Display,
    {
        match &error {
            SdkError::ServiceError(service_error) => {
                let raw_response = service_error.raw();
                let status_code = raw_response.status().as_u16();
                let headers = raw_response.headers();
                let body = raw_response.body().bytes().map(|bytes| {
                    String::from_utf8_lossy(bytes).to_string()
                }).unwrap_or_else(|| "<body not available>".to_string());

                tracing::error!(
                    error_type = "ServiceError",
                    operation = operation,
                    bucket = bucket,
                    key = key,
                    status_code = status_code,
                    headers = ?headers,
                    response_body = %body,
                    service_error = %service_error.err(),
                    "AWS S3 service error occurred"
                );

                match status_code {
                    404 => StoreError::DoesNotExist("Object not found".to_string()),
                    403 => StoreError::NotAuthorized("Access denied".to_string()),
                    401 => StoreError::NotAuthorized("Unauthorized".to_string()),
                    _ => StoreError::ConnectionError(format!("Service error: {}", service_error.err())),
                }
            }
            SdkError::TimeoutError(timeout_error) => {
                tracing::error!(
                    error_type = "TimeoutError",
                    operation = operation,
                    bucket = bucket,
                    key = key,
                    timeout_error = ?timeout_error,
                    "AWS S3 request timeout"
                );
                StoreError::ConnectionError("Request timeout".to_string())
            }
            SdkError::ResponseError(response_error) => {
                tracing::error!(
                    error_type = "ResponseError",
                    operation = operation,
                    bucket = bucket,
                    key = key,
                    response_error = ?response_error,
                    "AWS S3 response error"
                );
                StoreError::ConnectionError(format!("Response error: {:?}", response_error))
            }
            SdkError::DispatchFailure(dispatch_error) => {
                tracing::error!(
                    error_type = "DispatchFailure",
                    operation = operation,
                    bucket = bucket,
                    key = key,
                    dispatch_error = ?dispatch_error,
                    "AWS S3 dispatch failure"
                );
                StoreError::ConnectionError(format!("Dispatch failure: {:?}", dispatch_error))
            }
            SdkError::ConstructionFailure(construction_error) => {
                tracing::error!(
                    error_type = "ConstructionFailure",
                    operation = operation,
                    bucket = bucket,
                    key = key,
                    construction_error = ?construction_error,
                    "AWS S3 request construction failure"
                );
                StoreError::ConnectionError(format!("Construction failure: {:?}", construction_error))
            }
            _ => {
                tracing::error!(
                    error_type = "Other",
                    operation = operation,
                    bucket = bucket,
                    key = key,
                    error = %error,
                    "Unknown AWS S3 error"
                );
                StoreError::ConnectionError(format!("AWS SDK error: {}", error))
            }
        }
    }

    fn prefixed_key(&self, key: &str) -> String {
        if let Some(path_prefix) = &self.config.bucket_prefix {
            format!("{}/{}", path_prefix, key)
        } else {
            key.to_string()
        }
    }

    pub async fn init(&self) -> Result<()> {
        if self._bucket_checked.get().is_some() {
            return Ok(());
        }

        let client = self.get_client().await?;

        match client.head_bucket()
            .bucket(&self.config.bucket)
            .send()
            .await
        {
            Ok(_) => {
                self._bucket_checked.set(()).unwrap();
                Ok(())
            }
            Err(err) => {
                match Self::handle_aws_error(err, "head_bucket", &self.config.bucket, None) {
                    StoreError::DoesNotExist(_) => {
                        Err(StoreError::BucketDoesNotExist(
                            "Bucket does not exist.".to_string(),
                        ))
                    }
                    e => Err(e),
                }
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let client = self.get_client().await?;

        match client.get_object()
            .bucket(&self.config.bucket)
            .key(&prefixed_key)
            .send()
            .await
        {
            Ok(output) => {
                let bytes = output.body.collect().await
                    .map_err(|e| StoreError::ConnectionError(format!("Failed to read object body: {}", e)))?;
                Ok(Some(bytes.into_bytes().to_vec()))
            }
            Err(err) => {
                match Self::handle_aws_error(err, "get_object", &self.config.bucket, Some(&prefixed_key)) {
                    StoreError::DoesNotExist(_) => Ok(None),
                    e => Err(e),
                }
            }
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let client = self.get_client().await?;

        match client.put_object()
            .bucket(&self.config.bucket)
            .key(&prefixed_key)
            .body(ByteStream::from(value))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Self::handle_aws_error(err, "put_object", &self.config.bucket, Some(&prefixed_key))),
        }
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let client = self.get_client().await?;

        match client.delete_object()
            .bucket(&self.config.bucket)
            .key(&prefixed_key)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Self::handle_aws_error(err, "delete_object", &self.config.bucket, Some(&prefixed_key))),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let client = self.get_client().await?;

        match client.head_object()
            .bucket(&self.config.bucket)
            .key(&prefixed_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                match Self::handle_aws_error(err, "head_object", &self.config.bucket, Some(&prefixed_key)) {
                    StoreError::DoesNotExist(_) => Ok(false),
                    e => Err(e),
                }
            }
        }
    }

    fn snapshot_key(&self, base_key: &str, timestamp: u64) -> String {
        base_key.replace("/data.ysweet", &format!("/versions/version.{}.ysweet", timestamp))
    }

    fn snapshots_prefix(&self, base_key: &str) -> String {
        base_key.replace("/data.ysweet", "/versions/")
    }

    fn extract_timestamp_from_key(&self, key: &str) -> Option<u64> {
        // Extract timestamp from "prefix/versions/version.1234567890.ysweet"
        if let Some(filename) = key.split('/').last() {
            if let Some(timestamp_str) = filename.strip_prefix("version.").and_then(|s| s.strip_suffix(".ysweet")) {
                timestamp_str.parse().ok()
            } else {
                None
            }
        } else {
            None
        }
    }

    async fn create_snapshot_impl(&self, key: &str, timestamp: u64) -> Result<()> {
        // Copy current document to snapshot location
        if let Some(data) = self.get(key).await? {
            let snapshot_key = self.snapshot_key(key, timestamp);
            tracing::info!(key = %key, snapshot_key = %snapshot_key, timestamp = timestamp, "Creating snapshot");
            self.set(&snapshot_key, data).await?;
        }
        Ok(())
    }

    async fn list_snapshots_impl(&self, key: &str) -> Result<Vec<SnapshotInfo>> {
        let prefix = self.prefixed_key(&self.snapshots_prefix(key));
        let client = self.get_client().await?;

        let mut snapshots = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = client.list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(&prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await
                .map_err(|e| StoreError::ConnectionError(format!("Failed to list snapshots: {}", e)))?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let (Some(obj_key), Some(size)) = (object.key, object.size) {
                        if let Some(timestamp) = self.extract_timestamp_from_key(&obj_key) {
                            snapshots.push(SnapshotInfo {
                                timestamp,
                                size: size as usize,
                                hash: 0, // Could calculate if needed
                            });
                        }
                    }
                }
            }

            if response.is_truncated == Some(true) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        snapshots.sort_by_key(|s| s.timestamp);
        Ok(snapshots)
    }

    async fn get_snapshot_impl(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        tracing::info!(key = %key, snapshot_key = %snapshot_key, timestamp = timestamp, "Getting snapshot");
        self.get(&snapshot_key).await
    }

    async fn restore_from_snapshot_impl(&self, key: &str, timestamp: u64) -> Result<()> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        tracing::info!(key = %key, snapshot_key = %snapshot_key, timestamp = timestamp, "Restoring from snapshot");
        if let Some(snapshot_data) = self.get(&snapshot_key).await? {
            self.set(key, snapshot_data).await?;
        } else {
            return Err(StoreError::DoesNotExist(format!("Snapshot {} not found", timestamp)));
        }
        Ok(())
    }

    async fn delete_snapshot_impl(&self, key: &str, timestamp: u64) -> Result<()> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        tracing::info!(key = %key, snapshot_key = %snapshot_key, timestamp = timestamp, "Deleting snapshot");
        self.remove(&snapshot_key).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Store for S3Store {
    async fn init(&self) -> Result<()> {
        self.init().await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.set(key, value).await
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.remove(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.exists(key).await
    }

    async fn create_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        self.create_snapshot_impl(key, timestamp).await
    }

    async fn create_snapshot_with_data(&self, key: &str, timestamp: u64, data: Vec<u8>) -> Result<()> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        tracing::info!(key = %key, snapshot_key = %snapshot_key, timestamp = timestamp, size = data.len(), "Creating snapshot with Yjs data");
        self.set(&snapshot_key, data).await
    }

    async fn list_snapshots(&self, key: &str) -> Result<Vec<SnapshotInfo>> {
        self.list_snapshots_impl(key).await
    }

    async fn get_snapshot(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>> {
        self.get_snapshot_impl(key, timestamp).await
    }

    async fn restore_from_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        self.restore_from_snapshot_impl(key, timestamp).await
    }

    async fn delete_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        self.delete_snapshot_impl(key, timestamp).await
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl Store for S3Store {
    async fn init(&self) -> Result<()> {
        self.init().await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.set(key, value).await
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.remove(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.exists(key).await
    }

    async fn create_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        self.create_snapshot_impl(key, timestamp).await
    }

    async fn create_snapshot_with_data(&self, key: &str, timestamp: u64, data: Vec<u8>) -> Result<()> {
        let snapshot_key = self.snapshot_key(key, timestamp);
        tracing::info!(key = %key, snapshot_key = %snapshot_key, timestamp = timestamp, size = data.len(), "Creating snapshot with Yjs data");
        self.set(&snapshot_key, data).await
    }

    async fn list_snapshots(&self, key: &str) -> Result<Vec<SnapshotInfo>> {
        self.list_snapshots_impl(key).await
    }

    async fn get_snapshot(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>> {
        self.get_snapshot_impl(key, timestamp).await
    }

    async fn restore_from_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        self.restore_from_snapshot_impl(key, timestamp).await
    }

    async fn delete_snapshot(&self, key: &str, timestamp: u64) -> Result<()> {
        self.delete_snapshot_impl(key, timestamp).await
    }
}
