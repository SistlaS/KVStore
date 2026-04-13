// all times are in milliseconds

// raft related
pub const MIN_ELECTION_TIMEOUT: u64 = 5000;
pub const MAX_ELECTION_TIMEOUT: u64 = 2 * MIN_ELECTION_TIMEOUT;
pub const MAX_RETRIES_PER_CONN: u32 = 10;
pub const HEARTBEAT_INTERVAL: u64 = 500;
pub const LOGAPPEND_TIMEOUT: u64 = 8000; // 3 second
// client connectivity
pub const CLIENT_CONNECT_TIMEOUT: u64 = 1000;
pub const HEARTBEAT_RESP_TIMEOUT: u64 = 1500;
pub const RETRY_INTERVAL: u64 = 1000;
pub mod node;
pub mod service;
pub mod state;

pub mod raft_rpc {
    tonic::include_proto!("raft_rpc");
}
