use super::{Result, StoreError};
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
            ));

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
}
