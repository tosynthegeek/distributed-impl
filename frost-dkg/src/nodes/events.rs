use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map},
    error::Error,
};

use frost::keys::dkg::round2;
use frost_ristretto255::{self as frost, Identifier};
use futures::{
    StreamExt,
    channel::{mpsc, oneshot},
};
use libp2p::{
    PeerId, Swarm, kad,
    multiaddr::Protocol,
    request_response::{self, OutboundRequestId},
    swarm::SwarmEvent,
};
use tracing::{error, info};

use crate::{
    common::types::{
        Command, FrostEvent, MessageRequest, MessageResponse, Round2KadBehaviour,
        Round2KadBehaviourEvent,
    },
    errors::FrostError,
};

#[allow(dead_code)]
pub struct KadEventLoop {
    swarm: Swarm<Round2KadBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<FrostEvent>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
    pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<()>>,
    pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<HashSet<PeerId>>>,
    pending_request_file:
        HashMap<OutboundRequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
}

impl KadEventLoop {
    pub fn new(
        swarm: Swarm<Round2KadBehaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<FrostEvent>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            pending_dial: Default::default(),
            pending_start_providing: Default::default(),
            pending_get_providers: Default::default(),
            pending_request_file: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        packages: BTreeMap<Identifier, round2::Package>,
        expected_packages: usize,
    ) -> Result<BTreeMap<Identifier, round2::Package>, FrostError> {
        let mut received_packages = BTreeMap::new();
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    if let Some(package_info) = self.handle_event_with_collection(event, &packages).await? {
                        received_packages.insert(package_info.0, package_info.1);

                        if received_packages.len() >= expected_packages {
                            return Ok(received_packages);
                        }
                    }
                },
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c, &packages).await,
                    None => return Err(FrostError::OperationFailed("Command channel closed".into())),
                },
            }
        }
    }

    async fn handle_event_with_collection(
        &mut self,
        event: SwarmEvent<Round2KadBehaviourEvent>,
        packages: &BTreeMap<Identifier, round2::Package>,
    ) -> Result<Option<(Identifier, round2::Package)>, FrostError> {
        match event {
            SwarmEvent::Behaviour(Round2KadBehaviourEvent::Requestresponse(
                request_response::Event::Message { message, peer, .. },
            )) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    println!("Received request {:?} from peer {:?}", request, peer);
                    let peer_id_bytes = peer.to_bytes();
                    let identifier = frost::Identifier::derive(&peer_id_bytes)?;

                    let response = MessageResponse("Package received".to_string());
                    if let Err(e) = self
                        .swarm
                        .behaviour_mut()
                        .requestresponse
                        .send_response(channel, response)
                    {
                        error!(%peer, phase = "round2", error = ?e, "Failed to send response");
                    }

                    return Ok(Some((identifier, request.0)));
                }
                request_response::Message::Response { .. } => {
                    info!("Received acknowledgment from {}", peer);
                }
            },
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!(%peer_id, phase = "round2", "Connection established with peer");

                let peer_id_bytes = peer_id.to_bytes();
                let identifier = frost::Identifier::derive(&peer_id_bytes)?;
                let packake = match packages.get(&identifier) {
                    Some(pkg) => pkg,
                    None => {
                        error!(%peer_id, phase = "round2", "No package found for peer");
                        return Ok(None);
                    }
                };
                let request = MessageRequest(packake.clone());
                let req_id = self
                    .swarm
                    .behaviour_mut()
                    .requestresponse
                    .send_request(&peer_id, request);
                info!(%peer_id, %req_id, phase = "round2", "Sent package to peer");
            }
            _ => {}
        }
        Ok(None)
    }

    async fn handle_command(
        &mut self,
        command: Command,
        _packages: &BTreeMap<Identifier, round2::Package>,
    ) {
        match command {
            Command::StartListening { addr, sender } => {
                let _ = match self.swarm.listen_on(addr) {
                    Ok(_) => sender.send(Ok(())),
                    Err(e) => sender.send(Err(Box::new(e))),
                };
            }
            Command::Dial {
                peer_id,
                peer_addr,
                sender,
            } => {
                if let hash_map::Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, peer_addr.clone());
                    match self.swarm.dial(peer_addr.with(Protocol::P2p(peer_id))) {
                        Ok(()) => {
                            e.insert(sender);
                        }
                        Err(e) => {
                            let _ = sender.send(Err(Box::new(e)));
                        }
                    }
                }
            }
            Command::RequestMessage {
                peer,
                message,
                sender,
            } => {
                let request = MessageRequest(message);
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .requestresponse
                    .send_request(&peer, request);

                info!(%peer, %request_id, "Sent request to peer");
                self.pending_request_file.insert(request_id, sender);
            }
            Command::RespondMessage { peer, channel } => {
                let response = MessageResponse("Acknowledgment sent".to_string());
                if let Err(e) = self
                    .swarm
                    .behaviour_mut()
                    .requestresponse
                    .send_response(channel, response)
                {
                    error!(%peer, "Failed to send response: {:?}", e);
                }
            }
        }
    }
}
