use bincode;
use frost::keys::dkg::round1;
use frost_ristretto255::{self as frost, Identifier};
use libp2p::{Multiaddr, SwarmBuilder};
use libp2p::{
    PeerId,
    futures::StreamExt,
    gossipsub,
    gossipsub::{IdentTopic, MessageAuthenticity},
    identity, mdns, noise,
    swarm::Swarm,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::common::types::State;
use crate::errors::FrostError;

#[derive(NetworkBehaviour)]
pub struct MyBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

pub async fn setup_r1(
    _local_peer_id: PeerId,
    local_key: identity::Keypair,
) -> Result<(Swarm<MyBehaviour>, IdentTopic), FrostError> {
    let mut swarm: Swarm<MyBehaviour> = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| FrostError::OperationFailed(e.to_string()))?
        .with_quic()
        .with_behaviour(|key| {
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(message_id_fn)
                .build()
                .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?;

            let gossipsub = gossipsub::Behaviour::new(
                MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(MyBehaviour { gossipsub, mdns })
        })
        .map_err(|e| FrostError::BehaviourError(e.to_string()))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let tcp_listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0".parse()?;

    swarm.listen_on(tcp_listen_addr)?;

    let quic_listen_addr: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1".parse()?;
    swarm.listen_on(quic_listen_addr)?;

    let topic = gossipsub::IdentTopic::new("frost-dkg");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    Ok((swarm, topic))
}

pub async fn broadcast_r1(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    package: round1::Package,
) -> Result<(), FrostError> {
    let message = bincode::serialize(&package)?;
    match swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), message)
    {
        Ok(_) => {}
        Err(e) => warn!("Error during broadcasting: {:?}", e),
    }
    Ok(())
}

pub async fn receive_r1(
    _state: Arc<Mutex<State>>,
    nodes: usize,
    swarm: &mut Swarm<MyBehaviour>,
    local_peer_id: PeerId,
) -> Result<
    (
        BTreeMap<Identifier, round1::Package>,
        HashMap<Identifier, PeerId>,
    ),
    FrostError,
> {
    let mut received_r1_packages = BTreeMap::new();
    let mut idetifier_to_peers = HashMap::new();

    while received_r1_packages.len() < nodes {
        let event = swarm.select_next_some().await;

        match event {
            SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source,
                message_id: _,
                message,
            })) => {
                if propagation_source != local_peer_id {
                    let package: round1::Package = bincode::deserialize(&message.data)?;
                    let peer_id_bytes = propagation_source.to_bytes();
                    let identifier = frost::Identifier::derive(&peer_id_bytes)?;
                    info!("Recieved package from {:?}", identifier);
                    received_r1_packages.insert(identifier, package);

                    if !idetifier_to_peers.contains_key(&identifier) {
                        idetifier_to_peers.insert(identifier, propagation_source);
                    }
                }
            }
            SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for (peer_id, _) in peers {
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
            }
            SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(peers))) => {
                for (peer_id, _) in peers {
                    swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer_id);
                }
            }
            SwarmEvent::NewListenAddr { .. } => {}
            _ => {}
        }
    }

    Ok((received_r1_packages, idetifier_to_peers))
}
