use clap::{Parser, Subcommand};
use tonic::Request;

use crate::generated::raft::client::{
    DeleteRequest, GetClusterStatusRequest, GetRequest, PutRequest,
    client_service_client::ClientServiceClient,
};

pub mod generated {
    pub mod raft {
        pub mod internal {
            tonic::include_proto!("raft.internal");
        }
        pub mod client {
            tonic::include_proto!("raft.client");
        }
    }
}

#[derive(Parser)]
#[command(
    name = "raft-cli",
    version = "1.0",
    author = "You",
    about = "CLI to interact with Raft nodes"
)]
struct Cli {
    /// Address of the Raft node gRPC server
    #[arg(short, long, default_value = "http://localhost:50061")]
    address: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Put a key-value pair
    Put { key: String, value: i32 },
    /// Get a key
    Get {
        key: String,
        #[arg(short, long)]
        consistent: bool,
    },
    /// Delete a key
    Delete { key: String },
    /// Get cluster status
    Status,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let addr = "http://127.0.0.1:50061";
    let mut client = ClientServiceClient::connect(addr).await?;

    match cli.command {
        Commands::Put { key, value } => {
            let response = client.put(Request::new(PutRequest { key, value })).await?;
            println!("Put response: {:?}", response.into_inner());
        }
        Commands::Get { key, consistent } => {
            let response = client
                .get(Request::new(GetRequest {
                    key,
                    consistent_read: consistent,
                }))
                .await?;
            println!("Get response: {:?}", response.into_inner());
        }
        Commands::Delete { key } => {
            let response = client.delete(Request::new(DeleteRequest { key })).await?;
            println!("Delete response: {:?}", response.into_inner());
        }
        Commands::Status => {
            let response = client
                .get_cluster_status(Request::new(GetClusterStatusRequest {}))
                .await?;
            println!("Cluster status: {:?}", response.into_inner());
        }
    }

    Ok(())
}
