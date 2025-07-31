pub mod errors;
pub mod nodes_services;

pub mod internal_service {
    tonic::include_proto!("raft.internal");
}

pub mod client_service {
    tonic::include_proto!("raft.client");
}

use std::time::{Duration, Instant};

use crate::client_service::NodeState;
use crate::client_service::client_service_server::ClientServiceServer;
use crate::internal_service::log_entry::EntryType;
use crate::internal_service::raft_internal_service_server::RaftInternalServiceServer;
use crate::nodes_services::{
    node_client_service::ClientServiceImpl, node_internal_service::RaftInternalServiceImpl,
};
use tokio::time::{interval, timeout};
use tonic::transport::{Channel, Server};

use futures::future::join_all;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Status};
use tracing::{info, warn};

use crate::internal_service::raft_internal_service_client::RaftInternalServiceClient;
use crate::internal_service::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry, NodeRole, NodeStatus,
    RequestVoteRequest, RequestVoteResponse,
};

const MAX_BACKOFF_TIME: u64 = 30;
const INITIAL_DELAY_MS: u64 = 2;
const INTERVAL: u64 = 5; // seconds
const MAJORITY_SIZE: usize = 2; // Minimum majority size for a 3-node cluster

#[derive(Debug, Clone)]
pub struct RaftNode {
    pub id: String,
    pub internal_address: String,
    pub client_address: String,
    pub state: NodeState,
    pub role: NodeRole,
    pub term: i32,
    pub peers: Vec<NodeConfig>,
    pub log: Vec<LogEntry>,
    pub commit_index: i32,
    pub next_index: HashMap<String, i32>,
    pub match_index: HashMap<String, i32>,
    pub voted_for: Option<String>,
    pub last_heartbeat: Instant,
    pub election_timeout: Duration,
    pub peer_clients: HashMap<String, RaftInternalServiceClient<Channel>>,
    pub state_machine: HashMap<String, i32>,
    pub last_applied: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeConfig {
    pub id: String,
    pub internal_address: String,
    pub client_address: String,
    pub status: NodeStatus,
    pub state: NodeState,
    pub role: NodeRole,
    pub heartbeat: Instant,
}

impl RaftNode {
    pub async fn new(id: String, peers: Vec<NodeConfig>) -> Result<Arc<RwLock<Self>>, Status> {
        let filtered_peers = peers
            .clone()
            .into_iter()
            .filter(|peer| peer.id != id && peer.status == NodeStatus::Active)
            .collect::<Vec<_>>();
        let mut peer_clients = HashMap::new();
        let _ = tokio::time::sleep(Duration::from_secs(10));
        for peer in &filtered_peers {
            let addr = format!("http://{}", peer.internal_address);
            match RaftInternalServiceClient::connect(addr).await {
                Ok(client) => {
                    peer_clients.insert(peer.id.clone(), client);
                    info!("Connected to peer {}", peer.id);
                }
                Err(e) => {
                    warn!("Warning: Failed to connect to peer {}: {}", peer.id, e);
                }
            }
        }

        let node_config = peers
            .iter()
            .find(|p| p.id == id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("Node {} not found in peers", id)))?;

        let internal_address = node_config.internal_address;
        let client_address = node_config.client_address;
        let state = node_config.state;
        let role = node_config.role;
        let election_timeout = Duration::from_secs(rand::rng().random_range(20..50));

        let node = RaftNode {
            id,
            internal_address,
            client_address,
            peers: filtered_peers.clone(),
            term: 0,
            state,
            log: vec![],
            commit_index: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peer_clients,
            voted_for: None,
            role,
            last_heartbeat: Instant::now(),
            election_timeout,
            state_machine: HashMap::new(),
            last_applied: 0,
        };

        info!(
            "Connected to {} peers, {} nodes in config",
            node.peer_clients.len(),
            peers.len()
        );

        let node_arc = Arc::new(RwLock::new(node));

        let reconnect_node = node_arc.clone();
        let mut delay_ms = INITIAL_DELAY_MS;

        tokio::spawn(async move {
            loop {
                let mut node = reconnect_node.write().await;
                let peers = &node.peers.clone();
                let mut any_failed = false;
                info!("Attempting to reconnect to peers...");
                if peers.is_empty() || filtered_peers.len() == node.peer_clients.len() {
                    info!("No peers to reconnect to.");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    break;
                }

                for peer in peers {
                    if node.peer_clients.contains_key(&peer.id) {
                        continue;
                    }
                    let addr = format!("http://{}", peer.internal_address);
                    match RaftInternalServiceClient::connect(addr).await {
                        Ok(client) => {
                            info!("Reconnected to peer {}", peer.id);
                            node.peer_clients.insert(peer.id.clone(), client);
                        }
                        Err(e) => {
                            println!("Retry failed to connect to {}: {}", peer.id, e);
                            any_failed = true;
                        }
                    }
                }

                if any_failed {
                    delay_ms = (delay_ms * 2).min(MAX_BACKOFF_TIME);
                } else {
                    delay_ms = INITIAL_DELAY_MS;
                }

                drop(node);
                tokio::time::sleep(Duration::from_secs(delay_ms)).await;
            }
        });

        Ok(node_arc)
    }

    pub async fn send_append_entries(
        &self,
        recipient_id: &str,
        entries: Vec<LogEntry>,
    ) -> Result<AppendEntriesResponse, Status> {
        if self.state != NodeState::Leader {
            return Err(Status::failed_precondition("Not the leader"));
        }

        let mut client = self
            .peer_clients
            .get(recipient_id)
            .ok_or_else(|| Status::not_found(format!("No client for node {}", recipient_id)))?
            .clone();

        let next_index = self.next_index.get(recipient_id).copied().unwrap_or(1);
        let prev_log_index = next_index - 1;
        let mut prev_log_term = 0;
        if prev_log_index > 0 {
            if let Some(entry) = self.log.get((prev_log_index - 1) as usize) {
                prev_log_term = entry.term;
            }
        }

        info!(
            "Sending AppendEntries to {}: term={}, prev_log_index={}, prev_log_term={}, entries_count={}",
            recipient_id,
            self.term,
            prev_log_index,
            prev_log_term,
            entries.len()
        );

        let request = Request::new(AppendEntriesRequest {
            term: self.term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
            recipient_id: recipient_id.to_string(),
        });

        let response = client.append_entries(request).await?;
        Ok(response.into_inner())
    }

    pub async fn replicate_log(&mut self) -> Result<(), Status> {
        if self.state != NodeState::Leader {
            return Err(Status::failed_precondition("Not the leader"));
        }

        let peers = self.peers.clone();
        let mut tasks = vec![];
        let mut should_force_commit = true;

        for peer in peers {
            let node = self.clone();
            info!(
                "Replicating log to peer {}: term={}, log_length={}",
                peer.id,
                node.term,
                node.log.len()
            );
            let next_index = self.next_index.get(&peer.id).cloned().unwrap_or(1);
            let mut entries = if next_index <= self.log.len() as i32 {
                self.log[(next_index as usize - 1)..].to_vec()
            } else {
                vec![]
            };
            if entries.is_empty() {
                let commit_entry = LogEntry {
                    term: node.term,
                    index: node.log.len() as i32 + 1,
                    entry_type: Some(EntryType::NoOp(1)), // Leader heartbeat
                };

                should_force_commit = false;
                entries.push(commit_entry);
            }
            tasks.push(tokio::spawn(async move {
                let response = node.send_append_entries(&peer.id, entries).await;
                (peer.id, response)
            }));
        }

        let results = join_all(tasks).await;

        let mut majority_count = 1; // Count self as a majority
        for result in results {
            let (peer_id, response) = match result {
                Ok((peer_id, Ok(response))) => {
                    majority_count += 1;
                    (peer_id, response)
                }
                Ok((peer_id, Err(e))) => {
                    warn!("Error sending AppendEntries to {}: {}", peer_id, e);
                    continue;
                }
                Err(e) => {
                    warn!("Task failed: {}", e);
                    continue;
                }
            };

            if response.term > self.term {
                self.term = response.term;
                self.state = NodeState::Follower;
                self.voted_for = None;
                return Ok(());
            }
            if response.success {
                let next_index = self.next_index.get(&peer_id).copied().unwrap_or(1);
                self.match_index
                    .insert(peer_id.clone(), next_index - 1 + self.log.len() as i32);
                self.next_index
                    .insert(peer_id.clone(), next_index + self.log.len() as i32);
                self.commit_index = self
                    .commit_index
                    .max(self.match_index.get(&peer_id).copied().unwrap_or(0));
            } else {
                let next_index = self.next_index.get(&peer_id).copied().unwrap_or(1);
                self.next_index
                    .insert(peer_id, next_index.saturating_sub(1).max(1));
            }
        }

        if majority_count >= MAJORITY_SIZE && should_force_commit {
            self.replicate_forced_commit_log().await?;
        }

        Ok(())
    }

    pub async fn replicate_forced_commit_log(&mut self) -> Result<(), Status> {
        if self.state != NodeState::Leader {
            return Err(Status::failed_precondition("Not the leader"));
        }

        let peers = self.peers.clone();
        let mut tasks = vec![];

        for peer in peers {
            let node = self.clone();
            info!(
                "Replicating log to peer {}: term={}, log_length={}",
                peer.id,
                node.term,
                node.log.len()
            );

            let mut entries = vec![];
            let commit_entry = LogEntry {
                term: node.term,
                index: node.log.len() as i32 + 1,
                entry_type: Some(EntryType::NoOp(2)), // Forced commit
            };
            self.log.push(commit_entry.clone());

            entries.push(commit_entry);
            tasks.push(tokio::spawn(async move {
                let response = node.send_append_entries(&peer.id, entries).await;
                (peer.id, response)
            }));
        }

        let results = join_all(tasks).await;

        let mut majority_count = 0;
        for result in results {
            match result {
                Ok((peer_id, Ok(response))) => {
                    majority_count += 1;
                    (peer_id, response)
                }
                Ok((peer_id, Err(e))) => {
                    warn!(
                        "Error sending forced commit AppendEntries to {}: {}",
                        peer_id, e
                    );
                    continue;
                }
                Err(e) => {
                    warn!("Task failed: {}", e);
                    continue;
                }
            };
        }

        self.apply_committed_entries();

        info!(
            "Applied forced commit log entries, new commit index: {} and stage machine: {:?}",
            self.commit_index, self.state_machine
        );

        info!("Forced commit log replicated to {} peers", majority_count);

        Ok(())
    }

    pub async fn send_request_vote(
        &self,
        recipient_id: &str,
    ) -> Result<RequestVoteResponse, Status> {
        if self.state != NodeState::Candidate {
            return Err(Status::failed_precondition("Not a candidate"));
        }

        let mut client = self
            .peer_clients
            .get(recipient_id)
            .ok_or_else(|| Status::not_found(format!("No client for node {}", recipient_id)))?
            .clone();

        let last_log_index = self.log.len() as i32;
        let last_log_term = self.log.last().map(|entry| entry.term).unwrap_or(0);

        info!(
            "Sending RequestVote to {}: term={}, last_log_index={}, last_log_term={}",
            recipient_id, self.term, last_log_index, last_log_term
        );

        let request = Request::new(RequestVoteRequest {
            term: self.term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
        });

        let response = timeout(Duration::from_millis(1000), client.request_vote(request)).await;

        match response {
            Ok(Ok(response)) => {
                info!("Received vote response from {}", recipient_id);
                Ok(response.into_inner())
            }
            Ok(Err(e)) => {
                warn!("RequestVote failed for {}: {}", recipient_id, e);
                Err(e)
            }
            Err(_) => {
                warn!("RequestVote timed out for {}", recipient_id);
                Err(Status::deadline_exceeded(format!(
                    "RequestVote timeout for {}",
                    recipient_id
                )))
            }
        }
    }

    pub async fn start_election(&mut self) -> Result<(), Status> {
        self.term += 1;
        self.state = NodeState::Candidate;
        self.voted_for = Some(self.id.clone());
        self.last_heartbeat = Instant::now();

        let peers = self.peers.clone();
        let mut votes = 1;

        let (tx, mut rx) = tokio::sync::mpsc::channel(peers.len());

        for peer in peers {
            let tx = tx.clone();
            let node = self.clone();
            tokio::spawn(async move {
                let result = node.send_request_vote(&peer.id).await;
                let _ = tx.send((peer.id, result)).await;
            });
        }

        drop(tx); // Close sender so the loop below ends when all responses come in

        while let Some((peer_id, result)) = rx.recv().await {
            let response = match result {
                Ok(response) => response,
                Err(e) => {
                    warn!("Error sending RequestVote to {}: {}", peer_id, e);
                    continue;
                }
            };

            if response.term > self.term {
                self.term = response.term;
                self.state = NodeState::Follower;
                self.voted_for = None;
                return Ok(());
            }

            if response.vote_granted {
                votes += 1;
            }

            info!(
                "Received vote from {}: term={}, votes={} expecting {}",
                peer_id, self.term, votes, MAJORITY_SIZE
            );

            let id = self.id.clone();

            match votes >= MAJORITY_SIZE {
                true => {
                    self.state = NodeState::Leader;
                    self.voted_for = None;

                    for peer in &self.peers {
                        self.next_index
                            .insert(peer.id.clone(), self.log.len() as i32 + 1);
                        self.match_index.insert(peer.id.clone(), 0);
                    }

                    info!(
                        "Node {:?} won the election with {} votes in term {}",
                        self, votes, self.term
                    );

                    return Ok(());
                }
                false => info!("Node {} has not reached majority: {}", id, votes),
            }
        }

        Ok(())
    }

    pub fn apply_committed_entries(&mut self) {
        while self.commit_index > self.last_applied {
            let index = self.last_applied as usize;
            if let Some(entry) = self.log.get(index) {
                let entry_type = entry.entry_type.clone().unwrap();

                match entry_type {
                    EntryType::NoOp(_) => {
                        info!("No-op entry at index {}: {:?}", index, entry);
                    }
                    EntryType::ClientCommand(command) => match command.command_type {
                        0 => {
                            let key = command.key;
                            let value = command.value;
                            self.state_machine.insert(key, value);
                        }
                        1 => {
                            let key = command.key;
                            self.state_machine.remove(&key);
                        }
                        _ => {
                            let key = command.key;
                            self.state_machine.get(&key);
                        }
                    },
                    EntryType::ConfigChange(_) => {
                        info!("Configuration change entry at index {}: {:?}", index, entry);
                    }
                }
            }
            self.last_applied += 1;
        }

        info!(
            "Applied committed entries, new last_applied index: {}, state machine: {:?}",
            self.last_applied, self.state_machine
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let node_id = args.get(1).cloned().unwrap_or("node1".to_string());
    let internal_address = args
        .get(2)
        .cloned()
        .unwrap_or("127.0.0.1:50050".to_string());
    let client_address = args
        .get(3)
        .cloned()
        .unwrap_or("127.0.0.1:50060".to_string());

    let peers = vec![
        NodeConfig {
            id: "node1".to_string(),
            internal_address: "127.0.0.1:50051".to_string(),
            client_address: "127.0.0.1:50061".to_string(),
            status: NodeStatus::Active,
            role: NodeRole::VotingMember,
            state: NodeState::Leader,
            heartbeat: Instant::now(),
        },
        NodeConfig {
            id: "node2".to_string(),
            internal_address: "127.0.0.1:50052".to_string(),
            client_address: "127.0.0.1:50062".to_string(),
            status: NodeStatus::Active,
            role: NodeRole::VotingMember,
            state: NodeState::Follower,
            heartbeat: Instant::now(),
        },
        NodeConfig {
            id: "node3".to_string(),
            internal_address: "127.0.0.1:50053".to_string(),
            client_address: "127.0.0.1:50063".to_string(),
            status: NodeStatus::Active,
            role: NodeRole::VotingMember,
            state: NodeState::Follower,
            heartbeat: Instant::now(),
        },
        NodeConfig {
            id: "node4".to_string(),
            internal_address: "127.0.0.1:50054".to_string(),
            client_address: "127.0.0.1:50064".to_string(),
            status: NodeStatus::Active,
            role: NodeRole::VotingMember,
            state: NodeState::Follower,
            heartbeat: Instant::now(),
        },
    ];

    let node = match RaftNode::new(node_id.clone(), peers).await {
        Ok(node) => node,
        Err(e) => {
            warn!("Failed to create Raft node: {}", e);
            return Err(Status::internal(format!("Failed to create Raft node: {}", e)).into());
        }
    };

    let internal_node = node.clone();
    let internal_addr = match internal_address.parse() {
        Ok(addr) => addr,
        Err(e) => {
            warn!("Invalid internal address: {}", e);
            return Err(Status::invalid_argument(format!(
                "Invalid internal address: {}",
                internal_address
            ))
            .into());
        }
    };

    tokio::spawn(async move {
        info!("Starting internal service on {}", internal_addr);
        if let Err(e) = Server::builder()
            .add_service(RaftInternalServiceServer::new(RaftInternalServiceImpl {
                node: internal_node,
            }))
            .serve(internal_addr)
            .await
        {
            warn!("Failed to start internal service: {}", e);
        }
    });

    let client_node = node.clone();
    let client_addr = client_address.parse().unwrap();
    tokio::spawn(async move {
        info!("Starting client service on {}", client_addr);
        if let Err(e) = Server::builder()
            .add_service(ClientServiceServer::new(ClientServiceImpl {
                node: client_node,
            }))
            .serve(client_addr)
            .await
        {
            warn!("Failed to start client service: {}", e);
        }
    });

    let mut interval = interval(Duration::from_secs(INTERVAL));
    loop {
        interval.tick().await;
        let mut node = node.write().await;

        tracing::info!(
            "Node running on internal address {} and client  address {}",
            node.internal_address,
            node.client_address
        );

        match node.state {
            NodeState::Leader => {
                info!("Node {} is the leader", node.id);
                if let Err(e) = node.replicate_log().await {
                    warn!("Replication failed: {}", e);
                }
            }
            NodeState::Follower | NodeState::Candidate => {
                if node.last_heartbeat.elapsed() >= node.election_timeout {
                    if let Err(e) = node.start_election().await {
                        warn!("Election failed: {}", e);
                    }
                }
            }
        }
    }
}
