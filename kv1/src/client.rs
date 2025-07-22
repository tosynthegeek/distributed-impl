mod errors;
pub mod kv_1 {
    tonic::include_proto!("kv");
}

use std::time::Duration;

use clap::{Parser, Subcommand};
use kv_1::{GetRequest, PutRequest};
use tokio::time::sleep;
use tonic::{Code, Request, Response};
use tracing::info;

use crate::kv_1::{key_value_client::KeyValueClient, GetResponse, PutResponse};

pub struct KvServiceClient {
    inner: KeyValueClient<tonic::transport::Channel>,
}

impl KvServiceClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = KeyValueClient::connect(addr.to_string()).await?;
        Ok(Self { inner: client })
    }

    pub async fn put(
        &mut self,
        key: String,
        value: String,
        version: i32,
    ) -> Result<Response<PutResponse>, tonic::Status> {
        let req = PutRequest {
            key,
            value,
            version,
        };
        self.inner.put(Request::new(req)).await
    }

    pub async fn get(&mut self, key: String) -> Result<Response<GetResponse>, tonic::Status> {
        let req = GetRequest { key };
        self.inner.get(Request::new(req)).await
    }
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    method: Method,
}

#[derive(Subcommand)]
enum Method {
    Put {
        #[arg(short, long)]
        key: String,
        #[arg(long)]
        value: String,
        #[arg(long)]
        version: i32,
    },
    Get {
        #[arg(short, long)]
        key: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();

    let addr = format!("http://[::1]:20001");
    let mut client = KvServiceClient::connect(&addr).await?;

    match cli.method {
        Method::Put {
            key,
            value,
            version,
        } => {
            let mut delay_secs = 1;
            let max_delay_secs = 60;
            loop {
                let resp = client.put(key.clone(), value.clone(), version).await;
                match resp {
                    Ok(res) => {
                        tracing::info!("{:?}", res.into_inner());
                    }
                    Err(status) => match status.code() {
                        Code::Unavailable | Code::DeadlineExceeded => {
                            info!("Transient error, retrying...");
                            continue;
                        }
                        Code::Aborted | Code::Internal | Code::Unknown => {
                            info!("Transient error, retrying in {} seconds...", delay_secs);
                            let _ = sleep(Duration::from_secs(delay_secs));
                            delay_secs = (delay_secs * 2).min(max_delay_secs);
                        }
                        _ => {
                            eprintln!("Non-retryable error: {:?}", status);
                            return Err(format!("{}", status).into());
                        }
                    },
                }
            }
        }

        Method::Get { key } => {
            let resp = client.get(key).await?;
            tracing::info!("{:?}", resp.into_inner());
        }
    }

    Ok(())
}
