use std::fmt;
use thiserror::Error as ThisError;
use tonic::Status;

#[derive(ThisError, Debug)]
pub enum KvError {
    KeyNotFound(String),
    InvalidVersion { expected: i32, got: i32 },
    StorageError(String),
    InvalidKey(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::KeyNotFound(key) => write!(f, "Key '{}' not found", key),
            KvError::InvalidVersion { expected, got } => {
                write!(f, "Version mismatch: expected {}, got {}", expected, got)
            }
            KvError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            KvError::InvalidKey(key) => write!(f, "Invalid key: '{}'", key),
        }
    }
}

impl From<KvError> for Status {
    fn from(err: KvError) -> Self {
        match err {
            KvError::KeyNotFound(_) => Status::not_found(err.to_string()),
            KvError::InvalidVersion { .. } => Status::failed_precondition(err.to_string()),
            KvError::StorageError(_) => Status::internal(err.to_string()),
            KvError::InvalidKey(_) => Status::invalid_argument(err.to_string()),
        }
    }
}
