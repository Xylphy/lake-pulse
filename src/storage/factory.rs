use std::sync::Arc;

use super::config::StorageConfig;
use super::error::StorageResult;
use super::generic::GenericStorageProvider;
use super::provider::StorageProvider;

/// Factory for creating storage providers
pub struct StorageProviderFactory;

impl StorageProviderFactory {
    /// Create a storage provider from a configuration
    ///
    /// This factory creates a generic storage provider that works with any
    /// object_store backend (AWS S3, Azure, GCS, or local filesystem).
    pub async fn from_config(config: StorageConfig) -> StorageResult<Arc<dyn StorageProvider>> {
        let provider = GenericStorageProvider::new(config).await?;
        Ok(Arc::new(provider))
    }
}
