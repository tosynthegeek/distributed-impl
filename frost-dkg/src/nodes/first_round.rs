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
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::error::Error as StdError;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::sync::Mutex;

use crate::common::types::{Nodes, State};
use crate::errors::FrostError;

#[derive(NetworkBehaviour)]
pub struct MyBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

pub async fn init(
    state: Arc<Mutex<State>>,
    node_name: &str,
) -> Result<
    (
        PeerId,
        identity::Keypair,
        Identifier,
        HashMap<PeerId, Nodes>,
    ),
    Box<dyn StdError>,
> {
    let local_key = keypair_from_name(node_name);
    let local_peer_id = PeerId::from(local_key.public());

    println!("Local peer id for {}: {:?}", node_name, local_peer_id);
    println!("Local peer id: {:?}", local_peer_id);
    let peer_id_bytes = local_peer_id.to_bytes();
    let identifier = frost::Identifier::derive(&peer_id_bytes)?;

    {
        let state_guard = state.lock().await;
        let mut map = state_guard.identifier_to_peer_id.lock().await;
        map.insert(identifier, local_peer_id.clone());
    }

    let mut nodes: HashMap<PeerId, Nodes> = HashMap::new();
    let ports = vec![5001, 5002];
    let peers = vec![
        "12D3KooWQCkBm1BYtkHpocxCwMgR8yjitEeHGx8spzcDLGt2gkBm",
        "12D3KooWHbogA5Qvs9VdkjKi1HxXZ11GAT2B5ARtfhveA8sXKUPj",
    ];
    for (peer, port) in peers.iter().zip(ports.iter()) {
        let peer_id = PeerId::from_str(peer).expect("invalid peer id string");
        let addr = format!("/ip4/127.0.0.1/tcp/{}", port);
        let address: Multiaddr = addr.parse().expect("msg");
        let node = Nodes {
            peer_id,
            address,
            port: *port,
        };

        nodes.insert(peer_id, node);
    }

    Ok((local_peer_id, local_key, identifier, nodes))
}

pub async fn setup_r1(
    _local_peer_id: PeerId,
    local_key: identity::Keypair,
) -> Result<(Swarm<MyBehaviour>, IdentTopic), FrostError> {
    println!("Building Swarm...");
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

    let tcp_listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0".parse().map_err(|e| {
        FrostError::MultiAddressError(format!("Failed to parse tcp listen address: {}", e))
    })?;
    swarm.listen_on(tcp_listen_addr).map_err(|e| {
        FrostError::TransportError(format!("Failed to start listening on address: {}", e))
    })?;

    let quic_listen_addr: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1".parse().map_err(|e| {
        FrostError::MultiAddressError(format!("Failed to parse quic listen address: {}", e))
    })?;
    swarm.listen_on(quic_listen_addr).map_err(|e| {
        FrostError::TransportError(format!("Failed to start listening on address: {}", e))
    })?;

    println!("Swarm built, getting topic....");
    let topic = gossipsub::IdentTopic::new("frost-dkg");
    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

    Ok((swarm, topic))
}

pub async fn broadcast_r1(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    package: round1::Package,
) -> Result<(), FrostError> {
    let message = bincode::serialize(&package).map_err(|e| {
        FrostError::Round1Error(format!("Failed to serialize round 1 package: {}", e))
    })?;
    let _ = tokio::time::sleep(tokio::time::Duration::from_secs(10));
    match swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), message)
    {
        Ok(_) => println!("Broadcast done...."),
        Err(e) => println!("Error during broadcasting: {:?}", e),
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
                    let package: round1::Package =
                        bincode::deserialize(&message.data).map_err(|e| {
                            FrostError::Round1Error(format!(
                                "Failed to deserialize round 1 package: {}",
                                e
                            ))
                        })?;
                    let peer_id_bytes = propagation_source.to_bytes();
                    let identifier = frost::Identifier::derive(&peer_id_bytes)?;
                    println!("Recieved package from {:?}", identifier);
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
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Local node is listening on: {}", address);
            }
            _ => {}
        }
    }

    Ok((received_r1_packages, idetifier_to_peers))
}

fn keypair_from_name(name: &str) -> identity::Keypair {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let result = hasher.finalize();

    let seed: [u8; 32] = result.into();

    // Turn the seed into an Ed25519 SecretKey
    let secret = identity::ed25519::SecretKey::try_from_bytes(seed)
        .expect("Valid 32 bytes for Ed25519 secret key");
    let kp = identity::ed25519::Keypair::from(secret);

    identity::Keypair::from(kp)
}
