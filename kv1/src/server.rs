use std::sync::Arc;
mod errors;
pub mod kv_1 {
    tonic::include_proto!("kv");
}

use crate::errors::KvError;
use dashmap::DashMap;
use kv_1::key_value_server::{KeyValue, KeyValueServer};
use kv_1::{GetRequest, GetResponse, PutRequest, PutResponse};
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

#[derive(Debug, Default)]
pub struct MyKeyValue {
    kv_store: Arc<DashMap<String, Value>>,
}

#[derive(Debug, Default)]
pub struct Value {
    data: String,
    version: i32,
}

#[tonic::async_trait]
impl KeyValue for MyKeyValue {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        info!(
            "Recieved put request from {}",
            request.remote_addr().unwrap()
        );
        let put_request = request.into_inner();
        let new_version = match self.kv_store.get(&put_request.key) {
            Some(value) => {
                let version = value.version;
                if put_request.version != version {
                    return Err(KvError::InvalidVersion {
                        expected: version,
                        got: put_request.version,
                    }
                    .into());
                }

                version + 1
            }
            None => {
                if put_request.version.ne(&0) {
                    return Err(KvError::InvalidVersion {
                        expected: 0,
                        got: put_request.version,
                    }
                    .into());
                }

                1
            }
        };

        let value = Value {
            data: put_request.value,
            version: new_version,
        };

        self.kv_store.insert(put_request.key, value);
        let res = PutResponse { success: true };

        Ok(Response::new(res))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        info!(
            "Recieved get request from {}",
            request.remote_addr().unwrap()
        );
        let req = request.into_inner();

        match self.kv_store.get(&req.key) {
            Some(value) => {
                let res = GetResponse {
                    value: value.data.clone(),
                    version: value.version,
                    found: true,
                };

                Ok(Response::new(res))
            }
            None => {
                return Err(KvError::KeyNotFound(format!("{} is not a valid key", req.key)).into())
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let addr = "[::1]:20001".parse()?;

    let kv = MyKeyValue::default();

    tracing::info!("Server listening on {}..", addr,);

    Server::builder()
        .add_service(KeyValueServer::new(kv))
        .serve(addr)
        .await?;

    Ok(())
}
