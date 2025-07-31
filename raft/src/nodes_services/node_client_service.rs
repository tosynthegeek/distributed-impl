use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::{
    RaftNode,
    client_service::{
        DeleteRequest, DeleteResponse, GetClusterStatusRequest, GetClusterStatusResponse,
        GetRequest, GetResponse, NodeState, PartialNodeInfo, PutRequest, PutResponse,
        UpdateConfigRequest, UpdateConfigResponse, client_service_server::ClientService,
    },
    internal_service::{
        ClientCommand, ClusterConfiguration, CommandType, ConfigurationChange, LogEntry, NodeInfo,
        log_entry::EntryType,
    },
};

pub struct ClientServiceImpl {
    pub node: Arc<RwLock<RaftNode>>,
}

#[tonic::async_trait]
impl ClientService for ClientServiceImpl {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.write().await;

        info!(
            "Received PutRequest: key: {}, value: {}",
            req.key, req.value
        );

        if node.state != NodeState::Leader {
            return Ok(Response::new(PutResponse {
                success: false,
                error_message: "Not the leader".to_string(),
                leader_address: node
                    .clone()
                    .peers
                    .iter()
                    .find(|n| n.state == NodeState::Leader)
                    .map(|n| n.client_address.clone())
                    .unwrap_or_default(),
            }));
        }

        let term = node.term;
        let current_index = node.log.len() as i32 + 1; // 1-based index

        let command_type = CommandType::Put;
        let client_command = ClientCommand {
            key: req.key.clone(),
            value: req.value.clone(),
            command_type: command_type as i32,
        };

        node.log.push(LogEntry {
            term,
            index: current_index,
            entry_type: Some(EntryType::ClientCommand(client_command.clone())),
        });

        info!("Log entry added: {:?}", node.log.last());

        let leader_id = if node.state == NodeState::Leader {
            node.id.clone()
        } else {
            node.peers
                .iter()
                .find(|n| n.state == NodeState::Leader)
                .map(|n| n.client_address.clone())
                .unwrap_or_default()
        };

        if let Err(e) = node
            .replicate_log()
            .await
            .map_err(|e| Status::internal(e.to_string()))
        {
            return Ok(Response::new(PutResponse {
                success: false,
                error_message: format!("Replication failed: {}", e),
                leader_address: leader_id,
            }));
        }

        Ok(Response::new(PutResponse {
            success: true,
            error_message: String::new(),
            leader_address: leader_id,
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let node = self.node.read().await;

        info!("Received GetRequest: key: {}", req.key);

        let value = node
            .state_machine
            .get(&req.key)
            .cloned()
            .unwrap_or_default();

        let leader_id = if node.state == NodeState::Leader {
            node.id.clone()
        } else {
            node.peers
                .iter()
                .find(|n| n.state == NodeState::Leader)
                .map(|n| n.client_address.clone())
                .unwrap_or_default()
        };

        Ok(Response::new(GetResponse {
            success: true,
            value,
            error_message: String::new(),
            leader_address: leader_id,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.write().await;

        if node.state != NodeState::Leader {
            return Ok(Response::new(DeleteResponse {
                success: false,
                error: "Not the leader".to_string(),
            }));
        }

        // Serialize command
        let command_type = CommandType::Delete;
        let client_command = ClientCommand {
            key: req.key.clone(),
            value: 0,
            command_type: command_type as i32,
        };

        let term = node.term;
        let current_index = node.log.len() as i32 + 1;

        node.log.push(LogEntry {
            term,
            index: current_index,
            entry_type: Some(EntryType::ClientCommand(client_command.clone())),
        });

        // Trigger replication
        if let Err(e) = node.clone().replicate_log().await {
            return Ok(Response::new(DeleteResponse {
                success: false,
                error: format!("Replication failed: {}", e),
            }));
        }

        Ok(Response::new(DeleteResponse {
            success: true,
            error: String::new(),
        }))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<GetClusterStatusRequest>,
    ) -> Result<Response<GetClusterStatusResponse>, Status> {
        let node = self.node.read().await;

        info!("Received GetClusterStatusRequest");

        let mut nodes = Vec::new();
        nodes.push(PartialNodeInfo {
            node_id: node.id.clone(),
            address: node.client_address.clone(),
            state: node.state.clone() as i32,
            is_healthy: true,
        });

        let leader_id = if node.state == NodeState::Leader {
            node.id.clone()
        } else {
            node.peers
                .iter()
                .find(|n| n.state == NodeState::Leader)
                .map(|n| n.client_address.clone())
                .unwrap_or_default()
        };

        Ok(Response::new(GetClusterStatusResponse {
            nodes,
            leader_id,
            current_term: node.term,
            commit_index: node.commit_index,
        }))
    }

    async fn update_config(
        &self,
        request: Request<UpdateConfigRequest>,
    ) -> Result<Response<UpdateConfigResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.write().await;

        info!("Received UpdateConfigRequest: {:?}", req);

        let leader_id = if node.state == NodeState::Leader {
            node.id.clone()
        } else {
            node.peers
                .iter()
                .find(|n| n.state == NodeState::Leader)
                .map(|n| n.client_address.clone())
                .unwrap_or_default()
        };

        if node.state != NodeState::Leader {
            return Ok(Response::new(UpdateConfigResponse {
                success: false,
                error_message: "Not the leader".to_string(),
                leader_address: leader_id.clone(),
            }));
        }

        let term = node.term;
        let current_index = node.log.len() as i32 + 1;

        let (old_configuration, new_configuration) = match req.config_change.clone() {
            Some(change) => (change.old_configuration, change.new_configuration),
            None => return Err(Status::invalid_argument("Invalid config change")),
        };

        let old_cluster_config = match old_configuration {
            Some(config) => {
                let mut nodes = Vec::new();
                for node_info in config.nodes {
                    let node = NodeInfo {
                        node_id: node_info.node_id,
                        internal_address: node_info.internal_address,
                        client_address: node_info.client_address,
                        role: node_info.role as i32,
                        status: node_info.status as i32,
                    };
                    nodes.push(node);
                }
                ClusterConfiguration {
                    nodes,
                    configuration_id: config.configuration_id,
                    configuration_index: config.configuration_index,
                }
            }
            None => return Err(Status::invalid_argument("Old configuration is required")),
        };

        let new_cluster_config = match new_configuration {
            Some(config) => {
                let mut nodes = Vec::new();
                for node_info in config.nodes {
                    let node = NodeInfo {
                        node_id: node_info.node_id,
                        internal_address: node_info.internal_address,
                        client_address: node_info.client_address,
                        role: node_info.role as i32,
                        status: node_info.status as i32,
                    };
                    nodes.push(node);
                }
                ClusterConfiguration {
                    nodes,
                    configuration_id: config.configuration_id,
                    configuration_index: config.configuration_index,
                }
            }
            None => return Err(Status::invalid_argument("New configuration is required")),
        };

        let config_change = ConfigurationChange {
            old_configuration: Some(old_cluster_config),
            new_configuration: Some(new_cluster_config),
        };
        node.log.push(LogEntry {
            term: term,
            index: current_index,
            entry_type: Some(EntryType::ConfigChange(config_change)),
        });
        if let Err(e) = node.replicate_log().await {
            return Ok(Response::new(UpdateConfigResponse {
                success: false,
                error_message: format!("Replication failed: {}", e),
                leader_address: leader_id.clone(),
            }));
        }

        Ok(Response::new(UpdateConfigResponse {
            success: true,
            error_message: String::new(),
            leader_address: leader_id.clone(),
        }))
    }
}
