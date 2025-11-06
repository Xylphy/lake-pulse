use thiserror::Error;

/// Errors that can occur during storage operations
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;
