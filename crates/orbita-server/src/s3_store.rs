//! Refreshable AWS authentication over the frozen S3 object-store contract.

use crate::S3StorageConfig;

use async_trait::async_trait;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_types::region::Region;
use bytes::Bytes;
use orbita_objectstore::s3::{Credentials, S3Config, S3Store};
use orbita_objectstore::{ETag, ObjectError, ObjectMeta, ObjectResult, ObjectStore, Precondition};
use std::ops::Range;

/// An S3 store that resolves short-lived AWS credentials for every operation.
///
/// The AWS chain caches credentials until they approach expiry, so resolving
/// per operation is cheap and ensures a long-running node never keeps an
/// expired instance-profile or assumed-role session.
pub(crate) struct RefreshingS3Store {
    config: S3StorageConfig,
    credentials: SharedCredentialsProvider,
}

impl RefreshingS3Store {
    pub(crate) async fn connect(config: S3StorageConfig) -> ObjectResult<Self> {
        let sdk = aws_config::from_env()
            .region(Region::new(config.region.clone()))
            .load()
            .await;
        let credentials = sdk.credentials_provider().ok_or_else(|| {
            ObjectError::AccessDenied("AWS credential chain has no configured provider".to_string())
        })?;
        Ok(Self {
            config,
            credentials,
        })
    }

    async fn store(&self) -> ObjectResult<S3Store> {
        let credentials = self
            .credentials
            .provide_credentials()
            .await
            .map_err(|error| {
                ObjectError::AccessDenied(format!("loading AWS credentials: {error}"))
            })?;
        S3Store::connect(S3Config {
            endpoint: self.config.endpoint.clone(),
            bucket: self.config.bucket.clone(),
            region: self.config.region.clone(),
            credentials: Credentials {
                access_key_id: credentials.access_key_id().to_string(),
                secret_access_key: credentials.secret_access_key().to_string(),
                session_token: credentials.session_token().map(ToString::to_string),
            },
            force_path_style: self.config.force_path_style,
        })
    }
}

#[async_trait]
impl ObjectStore for RefreshingS3Store {
    async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
        self.store().await?.put(key, data).await
    }

    async fn put_if(
        &self,
        key: &str,
        data: Bytes,
        precondition: Precondition,
    ) -> ObjectResult<ETag> {
        self.store().await?.put_if(key, data, precondition).await
    }

    async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
        self.store().await?.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes> {
        self.store().await?.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> ObjectResult<ObjectMeta> {
        self.store().await?.head(key).await
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>> {
        self.store().await?.list(prefix).await
    }

    async fn delete(&self, key: &str) -> ObjectResult<()> {
        self.store().await?.delete(key).await
    }
}
