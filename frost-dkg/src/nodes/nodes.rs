use std::{collections::HashMap, str::FromStr, time::Duration};

use frost::keys::dkg::round2;
use frost_ristretto255::{self as frost};
use futures::{
    SinkExt, Stream,
    channel::{mpsc, oneshot},
};
use libp2p::{
    Multiaddr, PeerId, StreamProtocol,
    identity::Keypair,
    kad, noise,
    request_response::{self, ProtocolSupport, ResponseChannel},
    tcp, yamux,
};

use crate::{
    common::{
        constants::R2_SWARM_TIMEOUT,
        types::{Command, FrostEvent, MessageResponse, Round2KadBehaviour, State},
        utils::keypair_from_name,
    },
    errors::FrostError,
    nodes::events::KadEventLoop,
};

#[derive(Debug)]
pub struct Nodes {
    pub id: PeerId,
    pub keypair: Keypair,
    pub port: u16,
    pub address: Multiaddr,
    pub peers: HashMap<PeerId, Multiaddr>,
    pub sender: mpsc::Sender<Command>,
    pub state: State,
}

impl Nodes {
    pub async fn new(port: u16, name: &str, state: State) -> Result<Self, FrostError> {
        let keypair = keypair_from_name(name);
        let id = PeerId::from(keypair.public());
        let sender = {
            let (sender, _receiver) = mpsc::channel(32);
            sender
        };
        let addr = format!("/ip4/127.0.0.1/tcp/{}", port);
        let address: Multiaddr = addr.parse()?;
        let mut peers = HashMap::new();
        for (peer, port) in state.node_ids.iter().zip(state.node_ports.iter()) {
            let peer_id = PeerId::from_str(peer)?;
            let addr = format!("/ip4/127.0.0.1/tcp/{}", port);
            let address: Multiaddr = addr.parse()?;

            peers.insert(peer_id, address);
        }
        Ok(Nodes {
            id,
            port,
            address,
            peers,
            sender,
            keypair,
            state,
        })
    }

    pub async fn start_listening(&mut self) -> Result<(), FrostError> {
        let (sender, _) = oneshot::channel();
        let addr = self.address.clone();
        self.sender
            .send(Command::StartListening { addr, sender })
            .await?;
        // receiver.await.expect("Sender not to be dropped.")

        Ok(())
    }

    pub async fn dial(&mut self, peer_id: PeerId) -> Result<(), FrostError> {
        let (sender, _) = oneshot::channel();
        let peer_addr = match self.peers.get(&peer_id) {
            Some(addr) => addr.clone(),
            None => return Err(FrostError::OperationFailed("Peer address not found".into())),
        };
        self.sender
            .send(Command::Dial {
                peer_id,
                peer_addr,
                sender,
            })
            .await?;
        // receiver.await.expect("Sender not to be dropped.")
        Ok(())
    }

    pub async fn send_r2_package(
        &mut self,
        peer: PeerId,
        package: round2::Package,
    ) -> Result<(), FrostError> {
        let (sender, _) = oneshot::channel();
        self.sender
            .send(Command::RequestMessage {
                peer,
                sender,
                message: package,
            })
            .await?;
        // receiver.await.expect("Sender not be dropped.")

        Ok(())
    }

    pub async fn send_acknowlegement(
        &mut self,
        peer: PeerId,
        channel: ResponseChannel<MessageResponse>,
    ) -> Result<(), FrostError> {
        self.sender
            .send(Command::RespondMessage { peer, channel })
            .await?;

        Ok(())
    }

    pub async fn setup_r2(
        self,
    ) -> Result<(Self, impl Stream<Item = FrostEvent>, KadEventLoop), FrostError> {
        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(self.keypair.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|key| Round2KadBehaviour {
                kademlia: kad::Behaviour::new(
                    self.id,
                    kad::store::MemoryStore::new(key.public().to_peer_id()),
                ),
                requestresponse: request_response::cbor::Behaviour::new(
                    [(
                        StreamProtocol::new("/file-exchange/1"),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                ),
            })?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(Duration::from_secs(R2_SWARM_TIMEOUT))
            })
            .build();

        swarm
            .behaviour_mut()
            .kademlia
            .set_mode(Some(kad::Mode::Server));

        let (command_sender, command_receiver) = mpsc::channel(0);
        let (event_sender, event_receiver) = mpsc::channel(0);

        Ok((
            Self {
                sender: command_sender,
                ..self
            },
            event_receiver,
            KadEventLoop::new(swarm, command_receiver, event_sender),
        ))
    }
}
