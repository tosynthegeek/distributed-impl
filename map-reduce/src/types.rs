use serde::{Deserialize, Serialize};

use crate::master::MasterStatus;

#[derive(Deserialize, Serialize, Debug)]
pub enum MessageType {
    ConnectionMessage,
    ResponseMessage,
    TaskMessage,
    CompletionMessage,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Message {
    pub message_type: MessageType,
    pub payload: Option<String>,
    pub task_status: Option<MasterStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}
