use std::fmt;
use thiserror::Error as ThisError;
use tonic::Status;

#[derive(ThisError, Debug)]
pub enum RaftError {
    KeyNotFound(String),
    StorageError(String),
    InvalidKey(String),
    NotLeader,
}

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftError::KeyNotFound(key) => write!(f, "Key '{}' not found", key),
            RaftError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            RaftError::InvalidKey(key) => write!(f, "Invalid key: '{}'", key),
            RaftError::NotLeader => write!(f, "This node is not the leader"),
        }
    }
}

impl From<RaftError> for Status {
    fn from(err: RaftError) -> Self {
        match err {
            RaftError::KeyNotFound(_) => Status::not_found(err.to_string()),
            RaftError::StorageError(_) => Status::internal(err.to_string()),
            RaftError::InvalidKey(_) => Status::invalid_argument(err.to_string()),
            RaftError::NotLeader => Status::failed_precondition(err.to_string()),
        }
    }
}
