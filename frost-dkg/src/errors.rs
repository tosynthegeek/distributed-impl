use libp2p::{TransportError, gossipsub, identity::ParseError};

pub enum FrostError {
    InvalidInput(String),
    OperationFailed(String),
    NotFound(String),
    SubscriptionError(gossipsub::SubscriptionError),
    MultiAddressError(String),
    TransportError(String),
    Round2Error(String),
    Round1Error(String),
    BehaviourError(String),
    PeerNotFound(String),
    CommunicationError(String),
    PackageDeserializationError(String),
    ParseError(String),
    JoinError(tokio::task::JoinError),
}

impl std::fmt::Display for FrostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrostError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            FrostError::OperationFailed(msg) => write!(f, "Operation failed: {}", msg),
            FrostError::NotFound(msg) => write!(f, "Not found: {}", msg),
            FrostError::SubscriptionError(msg) => write!(f, "Subscription error: {}", msg),
            FrostError::MultiAddressError(msg) => write!(f, "MultiAddress error: {}", msg),
            FrostError::TransportError(msg) => write!(f, "Transport error: {}", msg),
            FrostError::Round2Error(msg) => write!(f, "Round 2 error: {}", msg),
            FrostError::Round1Error(msg) => write!(f, "Round 1 error: {}", msg),
            FrostError::BehaviourError(msg) => write!(f, "Behaviour error: {}", msg),
            FrostError::PeerNotFound(msg) => write!(f, "Peer not found: {}", msg),
            FrostError::CommunicationError(msg) => write!(f, "Communication error: {}", msg),
            FrostError::PackageDeserializationError(msg) => {
                write!(f, "Package deserialization error: {}", msg)
            }
            FrostError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            FrostError::JoinError(msg) => write!(f, "Join error: {}", msg),
        }
    }
}

impl std::fmt::Debug for FrostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrostError::InvalidInput(msg) => write!(f, "InvalidInput: {}", msg),
            FrostError::OperationFailed(msg) => write!(f, "OperationFailed: {}", msg),
            FrostError::NotFound(msg) => write!(f, "NotFound: {}", msg),
            FrostError::SubscriptionError(msg) => write!(f, "SubscriptionError: {}", msg),
            FrostError::MultiAddressError(msg) => write!(f, "MultiAddressError: {}", msg),
            FrostError::TransportError(msg) => write!(f, "TransportError: {}", msg),
            FrostError::Round2Error(msg) => write!(f, "Round2Error: {}", msg),
            FrostError::Round1Error(msg) => write!(f, "Round1Error: {}", msg),
            FrostError::BehaviourError(msg) => write!(f, "BehaviourError: {}", msg),
            FrostError::PeerNotFound(msg) => write!(f, "PeerNotFound: {}", msg),
            FrostError::CommunicationError(msg) => write!(f, "CommunicationError: {}", msg),
            FrostError::PackageDeserializationError(msg) => {
                write!(f, "PackageDeserializationError: {}", msg)
            }
            FrostError::ParseError(msg) => write!(f, "ParseError: {}", msg),
            FrostError::JoinError(msg) => write!(f, "JoinError: {}", msg),
        }
    }
}

impl std::error::Error for FrostError {}

impl From<frost_ristretto255::Error> for FrostError {
    fn from(err: frost_ristretto255::Error) -> Self {
        FrostError::OperationFailed(err.to_string())
    }
}

impl From<std::io::Error> for FrostError {
    fn from(err: std::io::Error) -> Self {
        FrostError::OperationFailed(err.to_string())
    }
}

impl From<libp2p::noise::Error> for FrostError {
    fn from(err: libp2p::noise::Error) -> Self {
        FrostError::TransportError(err.to_string())
    }
}

impl From<std::convert::Infallible> for FrostError {
    fn from(_: std::convert::Infallible) -> Self {
        FrostError::OperationFailed("Infallible error occurred (should never happen)".to_string())
    }
}

impl From<gossipsub::SubscriptionError> for FrostError {
    fn from(e: gossipsub::SubscriptionError) -> Self {
        FrostError::SubscriptionError(e)
    }
}

impl From<futures::channel::mpsc::SendError> for FrostError {
    fn from(err: futures::channel::mpsc::SendError) -> Self {
        FrostError::CommunicationError(err.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for FrostError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        FrostError::PackageDeserializationError(err.to_string())
    }
}

impl From<libp2p::multiaddr::Error> for FrostError {
    fn from(err: libp2p::multiaddr::Error) -> Self {
        FrostError::MultiAddressError(err.to_string())
    }
}

impl From<TransportError<std::io::Error>> for FrostError {
    fn from(err: TransportError<std::io::Error>) -> Self {
        FrostError::TransportError(err.to_string())
    }
}

impl From<ParseError> for FrostError {
    fn from(err: ParseError) -> Self {
        FrostError::ParseError(err.to_string())
    }
}

impl From<tokio::task::JoinError> for FrostError {
    fn from(err: tokio::task::JoinError) -> Self {
        FrostError::JoinError(err)
    }
}
