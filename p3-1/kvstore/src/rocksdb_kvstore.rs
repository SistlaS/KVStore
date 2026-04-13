use crate::kvstore::{KVStoreError, KVStoreTrait};
use rocksdb::{Direction, IteratorMode, Options, DB};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct RocksKVStore {
    db: Arc<RwLock<DB>>,
}

impl RocksKVStore {
    pub fn new(db_path: &str) -> Self {
        let path = Path::new(db_path);
        let mut options = Options::default();
        options.create_if_missing(true);
        // this is the default
        // options.set_manual_wal_flush(false);
        // options.set_wal_dir(db_path);

        let db = DB::open(&options, path).expect("Failed to open RocksDB");
        RocksKVStore {
            db: Arc::new(RwLock::new(db)),
        }
    }
}

#[async_trait::async_trait]
impl KVStoreTrait for RocksKVStore {
    async fn put(&self, key: String, value: String) -> Result<bool, KVStoreError> {
        let db = self.db.write().await;
        let exists = db.get(&key)?.is_some();
        db.put(key, value)?;
        db.flush_wal(true)?;
        Ok(exists)
    }

    async fn swap(&self, key: String, value: String) -> Result<Option<String>, KVStoreError> {
        let db = self.db.write().await;
        let raw_value = db.get(&key)?;

        let old_value = raw_value.map(String::from_utf8).transpose()?;

        db.put(key, value)?;
        db.flush_wal(true)?;
        Ok(old_value)
    }

    async fn get(&self, key: String) -> Result<Option<String>, KVStoreError> {
        let db = self.db.read().await;
        let raw_value = db.get(&key)?;
        raw_value
            .map(String::from_utf8)
            .transpose()
            .map_err(KVStoreError::from)
    }

    async fn scan(
        &self,
        start_key: String,
        end_key: String,
    ) -> Result<Vec<(String, String)>, KVStoreError> {
        let db = self.db.read().await;
        let mut result = Vec::new();
        let iter = db.iterator(IteratorMode::From(start_key.as_bytes(), Direction::Forward));

        for item in iter {
            let (key, value) = item?;
            let key = String::from_utf8(key.to_vec()).map_err(KVStoreError::Utf8Error)?;
            let value = String::from_utf8(value.to_vec()).map_err(KVStoreError::Utf8Error)?;
            if key > end_key {
                break;
            }
            result.push((key, value));
        }
        Ok(result)
    }

    async fn delete(&self, key: String) -> Result<bool, KVStoreError> {
        let db = self.db.write().await;
        let exists = db.get(&key)?.is_some();
        if exists {
            db.delete(&key)?;
            db.flush_wal(true)?;
        }
        Ok(exists)
    }

    async fn len(&self) -> Result<usize, KVStoreError> {
        let db = self.db.read().await;
        let count = db.iterator(IteratorMode::Start).count();
        Ok(count)
    }

    async fn is_empty(&self) -> Result<bool, KVStoreError> {
        self.len().await.map(|count| count == 0)
    }
}
