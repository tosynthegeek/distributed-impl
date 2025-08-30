use frost::keys::dkg::round2;
use frost_ristretto255::{self as frost};
use futures::channel::{mpsc, oneshot};
use libp2p::request_response::ResponseChannel;
use libp2p::{Multiaddr, kad, request_response};
use libp2p::{PeerId, swarm::NetworkBehaviour};
use serde::{Deserialize, Serialize};
use std::error::Error;

pub struct PeerDetails {
    pub address: String,
    pub state: NodeStatus,
}

pub enum NodeStatus {
    Active,
    Inactive,
    Failed,
}

#[derive(Debug, Clone)]
// A struct for the values that would be determined by the protocol and not from the nodes or code.
#[warn(dead_code)]
pub struct State {
    pub node_ids: Vec<String>,
    pub node_ports: Vec<u16>,
    pub max_signers: u16,
    pub min_signers: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Round2Request {
    SendPackage(round2::Package),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Round2Response {
    Acknowledgment(String),
}

#[derive(NetworkBehaviour)]
pub struct Round2KadBehaviour {
    pub requestresponse: request_response::cbor::Behaviour<MessageRequest, MessageResponse>,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
}

#[derive(Clone)]
pub struct Client {
    pub sender: mpsc::Sender<Command>,
}

#[derive(Debug)]
pub enum Command {
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    RequestMessage {
        peer: PeerId,
        message: round2::Package,
        sender: oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>,
    },
    RespondMessage {
        peer: PeerId,
        channel: ResponseChannel<MessageResponse>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRequest(pub round2::Package);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageResponse(pub String);

#[derive(Debug)]
pub enum FrostEvent {
    InboundRequest {
        request: MessageRequest,
        channel: ResponseChannel<MessageResponse>,
    },
}
