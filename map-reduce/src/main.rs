use std::sync::Arc;
use tokio::sync::Mutex;

use crate::master::MasterNode;
use crate::worker::WorkerNode;

use clap::{Parser, Subcommand};

pub mod master;
pub mod types;
pub mod worker;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    Master,
    Worker {
        #[arg(short, long)]
        port: u16,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();

    match cli.mode {
        Mode::Master => {
            let mut master = MasterNode::new();
            master.setup();
            let master = Arc::new(Mutex::new(master));
            MasterNode::run(master).await;
        }

        Mode::Worker { port } => {
            let worker = WorkerNode::new(port);
            let _ = worker.run().await;
        }
    }
}
