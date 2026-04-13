use crate::btree_kvstore::BTreeKVStore;
use crate::kvstore::{KVStoreError, KVStoreTrait};
pub mod logger {
    tonic::include_proto!("log");
}
use crc32fast::Hasher;
use log::debug;
use logger::{LogEntry, OperationType};
use prost::Message;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const LOGFILE_NAME: &str = "kv_ops.log";

pub struct DurableKVStore {
    inner_store: Arc<BTreeKVStore>,
    log: Arc<Mutex<BufWriter<File>>>,
    logpath: PathBuf,
}

impl DurableKVStore {
    /// Initialize KV store and replay Log to restore state
    pub async fn new(log_dir: &str) -> Self {
        let log_dirpath = Path::new(log_dir);
        if !log_dirpath.exists() {
            create_dir_all(log_dirpath).expect("Failed to create Log directory");
        }

        let logpath = log_dirpath.join(LOGFILE_NAME);

        let logfile = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&logpath)
            .expect("Failed to open Log file");

        let store = DurableKVStore {
            inner_store: Arc::new(BTreeKVStore::new()),
            log: Arc::new(Mutex::new(BufWriter::new(logfile))),
            logpath: logpath.clone(),
        };

        // Recover KVstore from Log
        store.replay_log().await;
        store
    }

    /// Logs an operation before applying it to state machine
    async fn log_a_op(&self, entry: &LogEntry) {
        let mut buffer = Vec::new();
        entry
            .encode(&mut buffer)
            .expect("Failed to encode Log entry");

        let mut hasher = Hasher::new();
        hasher.update(&buffer);
        let checksum = hasher.finalize();
        debug!("logging len {} | checksum {}", buffer.len(), checksum);

        let mut log = self.log.lock().await;

        // Writing data to the WAL
        log.write_all(&(buffer.len() as u32).to_le_bytes())
            .expect("Failed to write size");
        log.write_all(&buffer).expect("Failed to write Log entry");
        log.write_all(&checksum.to_le_bytes())
            .expect("Failed to write checksum");

        log.flush().expect("Failed to flush Log");
        log.get_ref().sync_all().expect("Failed to sync log file"); // Ensure disk persistence
        debug!("Logged entry: {:?}", entry);
    }

    /// Restore KVstore
    async fn replay_log(&self) {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.logpath.clone())
        {
            Ok(file) => file,
            Err(_) => return, // Nothing to recover
        };

        let mut reader = BufReader::new(&file);
        let mut position: u64;
        let mut i: usize = 0;

        loop {
            position = reader.seek(SeekFrom::Current(0)).unwrap();
            debug!("position {}", position);

            // check EOF before
            if reader.fill_buf().expect("Failed to read buffer").is_empty() {
                debug!("Reached EOF at {}", position);
                break;
            }

            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                println!(
                    "Incomplete Log entry detected. Truncating at position {}",
                    position
                );
                file.set_len(position).expect("Failed to truncate log");
                break;
            }

            let size = u32::from_le_bytes(size_buf) as usize;
            let mut buffer = vec![0; size];

            debug!("len {}", size);

            if reader.read_exact(&mut buffer).is_err() {
                println!(
                    "Incomplete Log entry detected. Truncating at position {}",
                    position
                );
                file.set_len(position).expect("Failed to truncate log");
                break;
            }

            let mut checksum_buf = [0u8; 4];
            if reader.read_exact(&mut checksum_buf).is_err() {
                println!("Missing checksum, truncating log at position {}", position);
                file.set_len(position).expect("Failed to truncate log");
                break;
            }

            let stored_checksum = u32::from_le_bytes(checksum_buf);

            let mut hasher = Hasher::new();
            hasher.update(&buffer);
            let computed_checksum = hasher.finalize();

            debug!(
                "checksum: stored {} | computed {} | len {}",
                stored_checksum, computed_checksum, size
            );

            if computed_checksum != stored_checksum {
                println!("Checksum mismatch, truncating log at position {}", position);
                file.set_len(position).expect("Failed to truncate log");
                break;
            }

            let entry = LogEntry::decode(&buffer[..]).expect("Failed to decode Log entry");

            match entry.op_type {
                0 => {
                    // PUT
                    debug!(
                        "{i} found PUT: {} {} pos {position}",
                        entry.key, entry.value
                    );
                    self.inner_store
                        .put(entry.key.clone(), entry.value.clone())
                        .await
                        .expect("Failed to apply PUT");
                }
                1 => {
                    // DELETE
                    debug!(
                        "{i} found DELETE: {} {} pos {position}",
                        entry.key, entry.value
                    );
                    self.inner_store
                        .delete(entry.key.clone())
                        .await
                        .expect("Failed to apply DELETE");
                }
                _ => println!("Unknown WAL operation"),
            }
            i += 1;
        }

        println!("Log replay complete.");
    }
}

#[async_trait::async_trait]
impl KVStoreTrait for DurableKVStore {
    async fn put(&self, key: String, value: String) -> Result<bool, KVStoreError> {
        let entry = LogEntry {
            op_type: OperationType::Put as i32,
            key: key.clone(),
            value: value.clone(),
        };

        self.log_a_op(&entry).await; // Log first
        self.inner_store.put(key, value).await
    }

    async fn get(&self, key: String) -> Result<Option<String>, KVStoreError> {
        self.inner_store.get(key).await
    }

    async fn swap(&self, key: String, value: String) -> Result<Option<String>, KVStoreError> {
        let entry = LogEntry {
            op_type: OperationType::Put as i32,
            key: key.clone(),
            value: value.clone(),
        };

        self.log_a_op(&entry).await; // Log first
        self.inner_store.swap(key, value).await
    }

    async fn scan(
        &self,
        start_key: String,
        end_key: String,
    ) -> Result<Vec<(String, String)>, KVStoreError> {
        self.inner_store.scan(start_key, end_key).await
    }

    async fn delete(&self, key: String) -> Result<bool, KVStoreError> {
        let entry = LogEntry {
            op_type: OperationType::Delete as i32,
            key: key.clone(),
            value: String::new(),
        };

        self.log_a_op(&entry).await;
        self.inner_store.delete(key).await
    }

    async fn len(&self) -> Result<usize, KVStoreError> {
        self.inner_store.len().await
    }

    async fn is_empty(&self) -> Result<bool, KVStoreError> {
        self.inner_store.is_empty().await
    }
}
