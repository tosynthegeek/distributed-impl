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

use crate::common::types::{
    Command, FrostEvent, MessageRequest, MessageResponse, Round2KadBehaviour,
    Round2KadBehaviourEvent,
};

#[allow(dead_code)]
pub struct EventLoop {
    swarm: Swarm<Round2KadBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<FrostEvent>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
    pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<()>>,
    pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<HashSet<PeerId>>>,
    pending_request_file:
        HashMap<OutboundRequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
}

impl EventLoop {
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
    ) -> Result<BTreeMap<Identifier, round2::Package>, Box<dyn Error + Send>> {
        let mut received_packages = BTreeMap::new();
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    if let Some(package_info) = self.handle_event_with_collection(event, &packages).await {
                        received_packages.insert(package_info.0, package_info.1);

                        // Check if we've received all expected packages
                        if received_packages.len() >= expected_packages {
                            return Ok(received_packages);
                        }
                    }
                },
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c, &packages).await,
                    None => return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Command channel closed"
                    ))),
                },
            }
        }
    }

    async fn handle_event_with_collection(
        &mut self,
        event: SwarmEvent<Round2KadBehaviourEvent>,
        packages: &BTreeMap<Identifier, round2::Package>,
    ) -> Option<(Identifier, round2::Package)> {
        match event {
            SwarmEvent::Behaviour(Round2KadBehaviourEvent::Requestresponse(
                request_response::Event::Message { message, peer, .. },
            )) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    println!("Received request {:?} from peer {:?}", request, peer);
                    let peer_id_bytes = peer.to_bytes();
                    let identifier = frost::Identifier::derive(&peer_id_bytes)
                        .expect("Failed to derive identifier");

                    let response = MessageResponse("Package received".to_string());
                    if let Err(e) = self
                        .swarm
                        .behaviour_mut()
                        .requestresponse
                        .send_response(channel, response)
                    {
                        eprintln!("Failed to send response: {:?}", e);
                    }

                    return Some((identifier, request.0));
                }
                request_response::Message::Response { response, .. } => {
                    println!("Received acknowledgment from {}: {:?}", peer, response.0);
                }
            },
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                println!("Connection established with peer: {:?}", peer_id);
                let peer_id_bytes = peer_id.to_bytes();
                let identifier =
                    frost::Identifier::derive(&peer_id_bytes).expect("Failed to derive identifier");
                let packake = packages.get(&identifier)?;
                let request = MessageRequest(packake.clone());
                let req_id = self
                    .swarm
                    .behaviour_mut()
                    .requestresponse
                    .send_request(&peer_id, request);
                println!(
                    "Sent package to peer {:?}, request ID: {:?}",
                    peer_id, req_id
                );
            }
            _ => {
                println!("Unhandled event: {:?}", event);
            }
        }
        None
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
                } else {
                    todo!("Already dialing peer.");
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

                println!(
                    "Sent package to peer {:?}, request ID: {:?}",
                    peer, request_id
                );
                // Store the sender to respond when we get a response
                self.pending_request_file.insert(request_id, sender);
            }
            Command::RespondMessage { peer, channel } => {
                println!("Peer: {}", peer);
                let response = MessageResponse("Acknowledgment sent".to_string());
                if let Err(e) = self
                    .swarm
                    .behaviour_mut()
                    .requestresponse
                    .send_response(channel, response)
                {
                    eprintln!("Failed to send response: {:?}", e);
                }
            }
        }
    }
}
