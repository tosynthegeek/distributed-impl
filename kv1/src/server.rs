use std::collections::HashMap;

pub mod kv_1 {
    tonic::include_proto!("kv");
}

use kv_1::key_value_server::{KeyValue, KeyValueServer};
use kv_1::{GetRequest, GetResponse, PutRequest, PutResponse};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug, Default)]
pub struct MyKeyValue {
    kv_store: HashMap<String, Value>,
}

#[derive(Debug, Default)]
pub struct Value {
    data: String,
    version: i32,
}

#[tonic::async_trait]
impl KeyValue for MyKeyValue {
    async fn put(&mut self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        println!("Got a request: {:?}", request);

        let put_request = request.into_inner();
        let new_version = match self.kv_store.get(&put_request.key) {
            Some(value) => {
                let version = value.version;
                if put_request.version != version {
                    return Err(()); // Version Error
                }

                version + 1
            }
            None => {
                if put_request.version.ne(&0) {
                    return Err(()); // Version Err
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
}
