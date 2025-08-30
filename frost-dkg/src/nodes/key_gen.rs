use frost::keys::dkg::{round1, round2};
use frost::keys::{KeyPackage, PublicKeyPackage};
use frost::{Error, Identifier};
use frost_ristretto255 as frost;
use futures::{Stream, StreamExt};
use libp2p::Multiaddr;
use libp2p::{PeerId, gossipsub::IdentTopic, identity, swarm::Swarm};
use rand::rngs::OsRng;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;
use tracing::{error, warn};

use crate::common::types::{FrostEvent, State};
use crate::errors::FrostError;
use crate::nodes::events::KadEventLoop;
use crate::nodes::first_round::{MyBehaviour, broadcast_r1, receive_r1};
use crate::nodes::nodes::Nodes;

pub async fn perform_round_one(
    nodes: usize,
    _local_key: identity::Keypair,
    topic: &IdentTopic,
    mut swarm: Swarm<MyBehaviour>,
    peer_id: PeerId,
    state: Arc<Mutex<State>>,
) -> Result<
    (
        round1::SecretPackage,
        round1::Package,
        BTreeMap<Identifier, round1::Package>,
        HashMap<Identifier, PeerId>,
    ),
    FrostError,
> {
    let rng = OsRng;
    let state_guard = state.lock().await;
    let max_signers = state_guard.max_signers;
    let min_signers = state_guard.min_signers;
    drop(state_guard);

    let peer_id_bytes = peer_id.to_bytes();
    let identifier = frost::Identifier::derive(&peer_id_bytes)?;
    let (first_secret_package, round1_package) =
        frost::keys::dkg::part1(identifier, max_signers, min_signers, rng)?;

    broadcast_r1(&mut swarm, &topic, round1_package.clone()).await?;

    let (receive_round_one_packages, identifier_to_peers) =
        receive_r1(state, nodes, &mut swarm, peer_id).await?;

    let mut round1_packages = BTreeMap::new();
    for (key, value) in receive_round_one_packages.clone() {
        let round1_pkg = value;
        round1_packages.insert(key, round1_pkg);
    }

    Ok((
        first_secret_package,
        round1_package,
        receive_round_one_packages,
        identifier_to_peers,
    ))
}

pub async fn perform_round_two(
    nodes: usize,
    mut node: Nodes,
    mut network_events: impl Stream<Item = FrostEvent> + Unpin,
    network_event_loop: KadEventLoop,
    packages: BTreeMap<Identifier, round2::Package>,
    identifier_to_peers: HashMap<Identifier, PeerId>,
    _peer_to_addr: HashMap<PeerId, Multiaddr>,
) -> Result<BTreeMap<Identifier, round2::Package>, FrostError> {
    let event_loop_handle = spawn(network_event_loop.run(packages.clone(), nodes));

    node.start_listening().await?;

    for (identifier, peer_id) in &identifier_to_peers {
        if let Some(package) = packages.get(identifier) {
            if let Err(e) = node.dial(*peer_id).await {
                warn!(%peer_id, phase = "round2", error = ?e, "Failed to dial peer");
                continue;
            }

            match node.send_r2_package(*peer_id, package.clone()).await {
                Ok(_) => {}
                Err(e) => {
                    error!(%peer_id, phase = "round2", error = ?e, "Failed to send package");
                }
            }
        }
    }

    let mut received_packages = Vec::new();

    while received_packages.len() < nodes - 1 {
        if let Some(event) = network_events.next().await {
            match event {
                FrostEvent::InboundRequest { request, .. } => {
                    received_packages.push(request.0);
                }
            }
        }
    }
    let collected_packages = event_loop_handle.await??;

    Ok(collected_packages)
}

pub async fn finalize_key_generation(
    sp2: round2::SecretPackage,
    received_round1_packages: &BTreeMap<Identifier, round1::Package>,
    received_round2_packages: &BTreeMap<Identifier, round2::Package>,
) -> Result<(KeyPackage, PublicKeyPackage), Error> {
    let (key_package, pubkey_package) =
        frost::keys::dkg::part3(&sp2, &received_round1_packages, &received_round2_packages)?;

    Ok((key_package, pubkey_package))
}
