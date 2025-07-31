use std::{sync::Arc, time::Instant};

use crate::{
    RaftNode,
    client_service::NodeState,
    internal_service::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse, log_entry::EntryType,
        raft_internal_service_server::RaftInternalService,
    },
};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct RaftInternalServiceImpl {
    pub node: Arc<RwLock<RaftNode>>,
}

#[tonic::async_trait]
impl RaftInternalService for RaftInternalServiceImpl {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let recipient_id = req.recipient_id.clone();
        let mut node = self.node.write().await;

        if recipient_id != node.id {
            return Err(Status::invalid_argument(format!(
                "Request intended for node {}, but this is node {}",
                recipient_id, node.id
            )));
        }

        info!(
            "Received AppendEntriesRequest from leader: {}, term: {}, prev_log_index: {}, prev_log_term: {}, entries: {:?}, leader_commit: {}",
            req.leader_id,
            req.term,
            req.prev_log_index,
            req.prev_log_term,
            req.entries,
            req.leader_commit
        );

        let mut current_term = node.term;

        if req.term < current_term {
            return Ok(Response::new(AppendEntriesResponse {
                term: current_term,
                success: false,
            }));
        }

        if req.term > current_term {
            node.term = req.term;
            node.state = NodeState::Follower;
            current_term = req.term;
        }

        if req.term >= current_term && node.state != NodeState::Follower {
            node.state = NodeState::Follower;
        }

        let log_ok = if req.prev_log_index == 0 {
            true
        } else if let Some(entry) = node.log.get(req.prev_log_index as usize - 1) {
            entry.term == req.prev_log_term
        } else {
            false
        };

        if !log_ok {
            return Ok(Response::new(AppendEntriesResponse {
                term: current_term,
                success: false,
            }));
        }

        let entry = req.entries.last().cloned().unwrap();
        let entry_type = entry.entry_type.clone().unwrap();
        match entry_type {
            EntryType::NoOp(e) => {
                info!("Received NoOp entry: {:?}", e);
                if e == 1 {
                    node.apply_committed_entries();
                }
            }
            _ => {
                info!("Received leader heartbeat log");
            }
        }

        if !req.entries.is_empty() {
            node.log.truncate(req.prev_log_index as usize);
            node.log.extend(req.entries);
        }
        if req.leader_commit > node.commit_index {
            node.commit_index = std::cmp::min(req.leader_commit, node.log.len() as i32);
        }

        Ok(Response::new(AppendEntriesResponse {
            term: current_term,
            success: true,
        }))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.write().await;

        let mut current_term = node.term;

        if req.term < current_term {
            return Ok(Response::new(RequestVoteResponse {
                term: current_term,
                vote_granted: false,
            }));
        }

        if req.term > current_term {
            node.term = req.term;
            node.state = NodeState::Follower;
            node.voted_for = None;
            current_term = req.term;
        }

        let can_vote =
            node.voted_for.is_none() || node.voted_for.as_ref() == Some(&req.candidate_id);
        let log_up_to_date = {
            let last_log_index = node.log.len() as i32;
            let last_log_term = node.log.last().map(|entry| entry.term).unwrap_or(0);
            req.last_log_term > last_log_term
                || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index)
        };

        if can_vote && log_up_to_date {
            node.voted_for = Some(req.candidate_id.clone());
            node.last_heartbeat = Instant::now();
            info!(
                "Node {} granted vote to candidate {}",
                node.id, req.candidate_id
            );
            Ok(Response::new(RequestVoteResponse {
                term: current_term,
                vote_granted: true,
            }))
        } else {
            Ok(Response::new(RequestVoteResponse {
                term: current_term,
                vote_granted: false,
            }))
        }
    }

    async fn install_snapshot(
        &self,
        _request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        Ok(Response::new(InstallSnapshotResponse {
            term: 0,
            success: false,
            bytes_received: 0,
            error_message: String::new(),
        }))
    }
}
