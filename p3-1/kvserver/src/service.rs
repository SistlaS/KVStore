use kvrpc::kv_service_server::{KvService, KvServiceServer};
use kvrpc::{
    DeleteReq, DeleteResp, GetReq, GetResp, KeyValuePair, LengthReq, LengthResp, PutReq, PutResp,
    ScanReq, ScanResp, SwapReq, SwapResp,
};
use kvstore::btree_kvstore::BTreeKVStore;
use kvstore::durable_kvstore::DurableKVStore;
use kvstore::kvstore::{KVStoreError, KVStoreTrait};
use kvstore::raft_kvstore::RaftKVStore;
// use kvstore::rocksdb_kvstore::RocksKVStore;
use log::{debug, error, warn, info};
use raft::node::{raft_write, RaftNode};
use tonic::{Response, Status};
pub mod kvrpc {
    tonic::include_proto!("kvrpc");
}
use crate::server::ServerConfig;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MyKVService {
    store: Box<dyn KVStoreTrait>,
    config: ServerConfig,
    raft_node: Arc<Mutex<RaftNode>>,
    req_lock: Mutex<()>,
}

use std::fs::OpenOptions;
use std::io::Write;

impl MyKVService {
    // Constructor
    pub async fn new(config: ServerConfig, raft_node: Arc<Mutex<RaftNode>>) -> Self {
        // let store: Box<dyn KVStoreTrait> = if config.use_rocksdb {
        //     let path = config.backer_path.clone();
        //     Box::new(RocksKVStore::new(&path))
        // } else {
        //     Box::new(BTreeKVStore::new())
        //     Box::new(DurableKVStore::new(&config.backer_path).await)
        // };
        // let store = Box::new(BTreeKVStore::new());
        let store = Box::new(RaftKVStore::new(&config.backer_path, raft_node.clone()).await);
        MyKVService {
            store,
            config,
            raft_node,
            req_lock: Mutex::new(()),
        }
    }

    pub async fn log_to_file(&self, msg: &str) {
        // get lock
        let _guard = self.req_lock.lock().await; // sequentialize requests
        let log_file = format!("req{}{}.log", self.config.part_id, self.config.rep_id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file)
            .expect("Failed to open log file");
        writeln!(
            file,
            "{} {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            msg
        )
        .expect("Failed to write to log file");
    }

    pub async fn get_service_server(
        config: ServerConfig,
        raft_node: Arc<Mutex<RaftNode>>,
    ) -> KvServiceServer<MyKVService> {
        let mykvservice = MyKVService::new(config, raft_node).await;
        return KvServiceServer::new(mykvservice);
    }
}

#[tonic::async_trait]
impl KvService for MyKVService {
    async fn put(
        &self,
        request: tonic::Request<PutReq>,
    ) -> Result<tonic::Response<PutResp>, tonic::Status> {
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let req = request.into_inner();
        info!("Request: PUT {} {}", req.key, req.value);

        if !self.config.key_valid(&req.key) {
            let errmsg = format!("Key `{}` not in my partition", req.key);
            error!("{}", errmsg);
            return Err(Status::invalid_argument(errmsg));
        }
        self.log_to_file(&format!("Request: PUT {} {}", req.key, req.value)).await;

        let oldval_exists = self.store.put(req.key, req.value).await;
        match oldval_exists {
            Ok(oldval_exists) => {
                let resp = PutResp {
                    found: oldval_exists,
                };
                info!("Response {:?}", resp);
                self.log_to_file(&format!("Response: {:?}", resp)).await;
                Ok(Response::new(resp))
            }
            Err(err) => {
                let errmsg = format!("PUT request failed. {}", err);
                warn!("{}", errmsg);
                self.log_to_file(&errmsg).await;
                Err(Status::internal(errmsg))
            }
        }
    }

    async fn swap(
        &self,
        request: tonic::Request<SwapReq>,
    ) -> Result<tonic::Response<SwapResp>, tonic::Status> {
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let req = request.into_inner();
        info!("Request: SWAP {} {}", req.key, req.value);

        if !self.config.key_valid(&req.key) {
            let errmsg = format!("Key `{}` not in my partition", req.key);
            error!("{}", errmsg);
            return Err(Status::invalid_argument(errmsg));
        }
        self.log_to_file(&format!("Request: SWAP {} {}", req.key, req.value)).await;

        let oldval = self.store.swap(req.key, req.value).await;
        match oldval {
            Ok(oldval) => {
                let found = oldval.is_some();
                let resp = SwapResp {
                    old_value: oldval.unwrap_or_else(|| "null".to_string()),
                    found: found,
                };
                info!("Response {:?}", resp);
                self.log_to_file(&format!("Response: {:?}", resp)).await;
                Ok(Response::new(resp))
            }
            Err(err) => {
                let errmsg = format!("SWAP request failed. {}", err);
                warn!("{}", errmsg);
                self.log_to_file(&errmsg).await;
                Err(Status::internal(errmsg))
            }
        }
    }

    async fn get(
        &self,
        request: tonic::Request<GetReq>,
    ) -> Result<tonic::Response<GetResp>, tonic::Status> {
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let req = request.into_inner();
        info!("Request: GET {}", req.key);

        // exception for __ping__ key
        let mut ping_key = false;
        if req.key.contains("__ping__") {
            ping_key = true;
        }
        self.log_to_file(&format!("Request: GET {}", req.key)).await;

        if !ping_key && !self.config.key_valid(&req.key) {
            let errmsg = format!("Key `{}` not in my partition", req.key);
            error!("{}", errmsg);
            return Err(Status::invalid_argument(errmsg));
        }

        let value = self.store.get(req.key).await;
        match value {
            Ok(value) => {
                let found = value.is_some();
                let resp = GetResp {
                    value: value.unwrap_or_else(|| "null".to_string()),
                    found: found,
                };
                info!("Response {:?}", resp);
                self.log_to_file(&format!("Response: {:?}", resp)).await;
                Ok(Response::new(resp))
            }
            Err(err) => {
                let errmsg = format!("GET request failed. {}", err);
                warn!("{}", errmsg);
                self.log_to_file(&errmsg).await;
                Err(Status::internal(errmsg))
            }
        }
    }

    async fn scan(
        &self,
        request: tonic::Request<ScanReq>,
    ) -> Result<tonic::Response<ScanResp>, tonic::Status> {
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let req = request.into_inner();
        info!("Request: SCAN {} {}", req.start_key, req.end_key);
        self.log_to_file(&format!("Request: SCAN {} {}", req.start_key, req.end_key)).await;

        let range = self.store.scan(req.start_key, req.end_key).await;
        match range {
            Ok(range) => {
                let mut result: Vec<KeyValuePair> = Vec::new();
                for (key, value) in range {
                    result.push(KeyValuePair { key, value });
                }
                debug!("res: {:?}", result);
                let resp = ScanResp { items: result };
                info!("Response {:?}", resp);
                self.log_to_file(&format!("Response: {:?}", resp)).await;
                Ok(Response::new(resp))
            }
            Err(err) => {
                let errmsg = format!("SCAN request failed. {}", err);
                warn!("{}", errmsg);
                self.log_to_file(&errmsg).await;
                Err(Status::internal(errmsg))
            }
        }
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteReq>,
    ) -> Result<tonic::Response<DeleteResp>, tonic::Status> {
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let req = request.into_inner();
        info!("Request: DELETE {}", req.key);

        if !self.config.key_valid(&req.key) {
            let errmsg = format!("Key `{}` not in my partition", req.key);
            error!("{}", errmsg);
            return Err(Status::invalid_argument(errmsg));
        }
        self.log_to_file(&format!("Request: DELETE {}", req.key)).await;

        let found = self.store.delete(req.key).await;
        match found {
            Ok(found) => {
                let resp = DeleteResp { found: found };
                info!("Response {:?}", resp);
                self.log_to_file(&format!("Response: {:?}", resp)).await;
                Ok(tonic::Response::new(resp))
            }
            Err(err) => {
                let errmsg = format!("DELETE request failed. {}", err);
                warn!("{}", errmsg);
                self.log_to_file(&errmsg).await;
                Err(Status::internal(errmsg))
            }
        }
    }

    // Caution: If the store length is greater than i64::MAX, this will not give correct result
    async fn length(
        &self,
        _request: tonic::Request<LengthReq>,
    ) -> Result<tonic::Response<LengthResp>, tonic::Status> {
        info!("Request: LENGTH");

        let length = self.store.len().await;
        match length {
            Ok(length) => {
                let length: i64 = length.try_into().unwrap_or(i64::MAX);
                let response = LengthResp { length: length };
                Ok(tonic::Response::new(response))
            }
            Err(err) => {
                let errmsg = format!("LENGTH request failed. {}", err);
                warn!("{}", errmsg);
                Err(Status::internal(errmsg))
            }
        }
    }
}
