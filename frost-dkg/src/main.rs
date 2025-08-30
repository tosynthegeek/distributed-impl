use std::sync::Arc;

use clap::Parser;
use frost_ristretto255 as frost;
use futures::StreamExt;
use libp2p::{Swarm, gossipsub::IdentTopic, mdns, swarm::SwarmEvent};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{
    common::{constants::WAIT_FOR_PEERS, types::State, utils::ProgressIndicator},
    errors::FrostError,
    nodes::{
        first_round::{MyBehaviour, MyBehaviourEvent, setup_r1},
        key_gen::{finalize_key_generation, perform_round_one, perform_round_two},
        nodes::Nodes,
    },
};

pub mod common;
pub mod errors;
pub mod nodes;

#[derive(Parser, Debug)]
#[command(author, version, about = "FROST DKG Node", long_about = None)]
struct Cli {
    #[arg(short, long)]
    name: String,

    #[arg(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), FrostError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(true)
        .init();

    let cli = Cli::parse();
    let name = cli.name;
    let port = cli.port;
    tracing::info!("Starting node on port: {}", port);

    let state_ports = vec![5001, 5002];
    let state = State {
        node_ids: vec![
            String::from("12D3KooWQCkBm1BYtkHpocxCwMgR8yjitEeHGx8spzcDLGt2gkBm"),
            String::from("12D3KooWHbogA5Qvs9VdkjKi1HxXZ11GAT2B5ARtfhveA8sXKUPj"),
        ],
        node_ports: state_ports.clone(),
        max_signers: 2,
        min_signers: 2,
    };

    if !state_ports.contains(&port) {
        tracing::error!(
            "Error: Port {} is not in the list of valid node ports: {:?}",
            port,
            state_ports
        );
        std::process::exit(1);
    }

    let dkg_process = ProgressIndicator::start("DKG process");
    let node = Nodes::new(port, &name, state).await?;

    let peer_id = node.id;
    let local_key = node.keypair.clone();
    let nodes_map = node.peers.clone();

    let node_state = node.state.clone();
    let state = Arc::new(Mutex::new(node_state));

    let setup = ProgressIndicator::start("Setup swarm + topic");
    let (mut swarm1, topic) = setup_r1(peer_id, local_key.clone()).await?;
    let nodess: usize = 1;
    setup.update("wait for peers to join");
    wait_for_peers(&topic, &mut swarm1, 1).await?;
    setup.finish();

    let round1 = ProgressIndicator::start("DKG Round 1");
    let (sp1, _round1_package, received_round1_packages, identifier_to_peers) = perform_round_one(
        nodess,
        local_key.clone(),
        &topic,
        swarm1,
        peer_id,
        state.clone(),
    )
    .await?;
    wait_for_round_completion(1, received_round1_packages.len(), nodess).await?;
    round1.finish();

    let round2 = ProgressIndicator::start("DKG Round 2");
    round2.update("Generate round 2 package");
    let (sp2, round2_package) = frost::keys::dkg::part2(sp1, &received_round1_packages)?;
    round2.update("Set up Kademlia");
    let (r2_node, network_events, network_event_loop) = node.setup_r2().await?;
    round2.update("Send round2 packages");
    let received_round2_packages = perform_round_two(
        nodess,
        r2_node,
        network_events,
        network_event_loop,
        round2_package.clone(),
        identifier_to_peers,
        nodes_map.clone(),
    )
    .await?;
    round2.finish();

    let finalize = ProgressIndicator::start("Finalize key generation...");
    let (_, _) =
        finalize_key_generation(sp2, &received_round1_packages, &received_round2_packages).await?;
    finalize.finish();

    dkg_process.finish();

    Ok(())
}

pub async fn wait_for_peers(
    topic: &IdentTopic,
    swarm: &mut Swarm<MyBehaviour>,
    min_peers: usize,
) -> Result<(), FrostError> {
    let mut discovered_peers = 0;
    let mut subscribed_peers: usize = 0;
    let topic_hash = topic.hash();
    let timeout_duration = tokio::time::Duration::from_secs(WAIT_FOR_PEERS);
    let start_time = tokio::time::Instant::now();

    while subscribed_peers < min_peers {
        if start_time.elapsed() >= timeout_duration {
            warn!("Timeout reached while waiting for peers.");
            break;
        }

        match swarm.select_next_some().await {
            SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for (peer_id, _) in &peers {
                    info!("New peer discovered peer");
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(peer_id);
                }
                discovered_peers += peers.len();
                info!(
                    "Discovered {} peers, total: {}",
                    peers.len(),
                    discovered_peers
                );
            }
            SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(
                libp2p::gossipsub::Event::Subscribed { peer_id: _, topic },
            )) => {
                if topic == topic_hash {
                    subscribed_peers += 1;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn wait_for_round_completion(
    round: u8,
    received_packages: usize,
    total_nodes: usize,
) -> Result<(), FrostError> {
    if received_packages < total_nodes - 1 {
        warn!(
            "Warning: Round {} completed with fewer packages than expected",
            round
        );
        info!(
            "Received {} packages, expected {}",
            received_packages,
            total_nodes - 1
        );
    }
    Ok(())
}
