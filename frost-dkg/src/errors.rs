pub enum FrostError {
    InvalidInput(String),
    OperationFailed(String),
    NotFound(String),
    SubscriptionError(String),
    MultiAddressError(String),
    TransportError(String),
    Round2Error(String),
    Round1Error(String),
    BehaviourError(String),
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
