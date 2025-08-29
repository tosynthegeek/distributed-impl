use futures::Stream;
use futures::channel::mpsc;
use libp2p::StreamProtocol;
use libp2p::kad;
use libp2p::request_response;
use libp2p::{PeerId, identity, noise, tcp, yamux};
use request_response::ProtocolSupport;
use std::error::Error as StdError;
use std::time::Duration;

use crate::common::types::{Client, FrostEvent, Round2KadBehaviour};
use crate::nodes::events::EventLoop;

pub async fn setup_r2(
    peer_id: PeerId,
    local_key: identity::Keypair,
) -> Result<(Client, impl Stream<Item = FrostEvent>, EventLoop), Box<dyn StdError>> {
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| Round2KadBehaviour {
            kademlia: kad::Behaviour::new(
                peer_id,
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
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    let (command_sender, command_receiver) = mpsc::channel(0);
    let (event_sender, event_receiver) = mpsc::channel(0);

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))
}
