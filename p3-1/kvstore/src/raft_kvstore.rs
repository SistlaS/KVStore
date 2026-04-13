use crate::kvstore::{KVStoreError, KVStoreTrait};
pub mod logger {
    tonic::include_proto!("log");
}

use log::debug;
use raft::node::{get_raft_state_machine, raft_is_leader, raft_write, RaftNode};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub enum Op {
    Put,
    Delete,
}

impl Op {
    pub fn to_string(&self) -> &'static str {
        match self {
            Op::Put => "0",
            Op::Delete => "1",
        }
    }
}

pub struct RaftKVStore {
    raft_node: Arc<Mutex<RaftNode>>,
    // req_lock: Mutex<()>,
}

impl RaftKVStore {
    /// Initialize KV store and replay Log to restore state
    pub async fn new(_log_dir: &str, raft_node: Arc<Mutex<RaftNode>>) -> Self {
        let store = RaftKVStore {
            raft_node,
            // req_lock: Mutex::new(()),
        };
        store
    }

    async fn ensure_leader(&self) -> Result<(), KVStoreError> {
        let (success, current_leader) = raft_is_leader(self.raft_node.clone()).await;
        if !success {
            let id = current_leader.map_or("-1".to_string(), |id| id.to_string());
            return Err(KVStoreError::RaftNotLeader(id));
        }
        Ok(())
    }

    async fn raft_put(&self, key: String, value: String) -> Result<bool, KVStoreError> {
        let entry = format!("PUT {} {}\n", key, value);
        let res = raft_write(self.raft_node.clone(), entry).await;
        if res.is_err() {
            return Err(KVStoreError::RaftWriteError(format!("{:?}", res.err())));
        }
        Ok(true)
    }
    async fn raft_get(&self, key: String) -> Result<Option<String>, KVStoreError> {
        let state_machine = get_raft_state_machine(self.raft_node.clone()).await;
        if let Ok(state_machine) = state_machine.clone().read() {
            return Ok(state_machine.get(&key).cloned());
        } else {
            return Err(KVStoreError::RaftSMError(
                "Failed to acquire read lock".to_string(),
            ));
        }
    }
}

#[async_trait::async_trait]
impl KVStoreTrait for RaftKVStore {
    async fn put(&self, key: String, value: String) -> Result<bool, KVStoreError> {
        debug!("waiting: PUT {} {}", key, value);
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let res = self.ensure_leader().await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        let old_val = self.raft_get(key.clone()).await;
        if old_val.is_err() {
            return Err(old_val.err().unwrap());
        }
        let old_val = old_val.unwrap();
        let key_exists = if old_val.is_some() { true } else { false };

        let res = self.raft_put(key.clone(), value.clone()).await;
        if res.is_err() {
            return Err(KVStoreError::RaftWriteError(format!("{:?}", res.err())));
        }

        return Ok(key_exists);
    }

    async fn get(&self, key: String) -> Result<Option<String>, KVStoreError> {
        debug!("waiting: GET {}", key);
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let res = self.ensure_leader().await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        let state_machine = get_raft_state_machine(self.raft_node.clone()).await;
        if let Ok(state_machine) = state_machine.clone().read() {
            return Ok(state_machine.get(&key).cloned());
        } else {
            return Err(KVStoreError::RaftSMError(
                "Failed to acquire read lock".to_string(),
            ));
        }
    }

    async fn swap(&self, key: String, value: String) -> Result<Option<String>, KVStoreError> {
        debug!("waiting: SWAP {} {}", key, value);
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let res = self.ensure_leader().await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        let old_value = self.raft_get(key.clone()).await;
        if old_value.is_err() {
            return Err(old_value.err().unwrap());
        }

        let res = self.raft_put(key.clone(), value.clone()).await;
        if res.is_err() {
            return Err(KVStoreError::RaftWriteError(format!("{:?}", res.err())));
        }

        let old_value = old_value.unwrap();
        return Ok(old_value);
    }

    async fn scan(
        &self,
        start_key: String,
        end_key: String,
    ) -> Result<Vec<(String, String)>, KVStoreError> {
        debug!("waiting: SCAN {} {}", start_key, end_key);
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let res = self.ensure_leader().await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        let state_machine = get_raft_state_machine(self.raft_node.clone()).await;
        if let Ok(state_machine) = state_machine.clone().read() {
            let mut result = Vec::new();
            let range = state_machine.range(start_key..=end_key);
            for (key, value) in range {
                result.push((key.clone(), value.clone()));
            }
            Ok(result)
        } else {
            return Err(KVStoreError::RaftSMError(
                "Failed to acquire read lock".to_string(),
            ));
        }
    }

    async fn delete(&self, key: String) -> Result<bool, KVStoreError> {
        debug!("waiting: DELETE {}", key);
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let res = self.ensure_leader().await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        let entry = format!("DEL {}\n", key);
        let old_val = self.raft_get(key.clone()).await;
        if old_val.is_err() {
            return Err(KVStoreError::Unknown(
                "Failed to check if key exists".to_string(),
            ));
        }
        let old_val = old_val.unwrap();
        let key_exists = if old_val.is_some() { true } else { false };

        let res = raft_write(self.raft_node.clone(), entry).await;
        if res.is_err() {
            return Err(KVStoreError::RaftWriteError(format!("{:?}", res.err())));
        }
        return Ok(key_exists);
    }

    async fn len(&self) -> Result<usize, KVStoreError> {
        debug!("waiting: LEN");
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        let res = self.ensure_leader().await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        let state_machine = get_raft_state_machine(self.raft_node.clone()).await;
        if let Ok(state_machine) = state_machine.clone().read() {
            return Ok(state_machine.len());
        } else {
            return Err(KVStoreError::RaftSMError(
                "Failed to acquire read lock".to_string(),
            ));
        }
    }

    async fn is_empty(&self) -> Result<bool, KVStoreError> {
        debug!("waiting: IS_EMPTY");
        // let _guard = self.req_lock.lock().await; // sequentialize requests
        self.len().await.map(|len| len == 0)
    }
}
