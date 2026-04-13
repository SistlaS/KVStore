use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KVStoreError {
    // #[error("Database error: {0}")]
    // DatabaseError(#[from] rocksdb::Error),
    #[error("UTF-8 decoding error")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    #[error("Raft write error: {0}")]
    RaftWriteError(String),

    #[error("Raft state machine error: {0}")]
    RaftSMError(String),

    #[error("NotLeader: {0}")]
    RaftNotLeader(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

#[async_trait]
pub trait KVStoreTrait: Send + Sync {
    async fn put(&self, key: String, value: String) -> Result<bool, KVStoreError>;
    async fn swap(&self, key: String, value: String) -> Result<Option<String>, KVStoreError>;
    async fn get(&self, key: String) -> Result<Option<String>, KVStoreError>;
    async fn scan(
        &self,
        start_key: String,
        end_key: String,
    ) -> Result<Vec<(String, String)>, KVStoreError>;
    async fn delete(&self, key: String) -> Result<bool, KVStoreError>;
    async fn len(&self) -> Result<usize, KVStoreError>;
    async fn is_empty(&self) -> Result<bool, KVStoreError>;
}
