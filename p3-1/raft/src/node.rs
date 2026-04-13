// #![allow(unused)]

use crate::raft_rpc::raft_service_client::RaftServiceClient;
use crate::raft_rpc::raft_service_server::RaftServiceServer;
use crate::raft_rpc::{AppendEntriesReq, AppendEntriesResp, LogEntry, RequestVoteReq};

use crate::service::MyRaftService;
use crate::state::{RaftState, Role};
use crate::{
    HEARTBEAT_INTERVAL, HEARTBEAT_RESP_TIMEOUT, LOGAPPEND_TIMEOUT, MAX_ELECTION_TIMEOUT,
    MAX_RETRIES_PER_CONN, MIN_ELECTION_TIMEOUT, RETRY_INTERVAL,
};
use dashmap::DashMap;
use log::{debug, info};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time::Instant;
use tokio::time::{Duration, sleep, timeout};
use tonic::{transport::Channel, transport::Server};

pub struct RaftNode {
    pub state: RwLock<RaftState>,
    // comm constants
    pub rf: u32,
    pub my_addr: String,
    pub my_id: u32,
    pub peer_addrs: Vec<String>,
    // additional volatile state
    pub peer_conns: DashMap<String, RaftServiceClient<Channel>>, // dashmap is a concurrent hashmap
    pub elec_timer_tx: Mutex<Option<mpsc::UnboundedSender<()>>>, // need lock since it might be reset, while someone else using it
    pub heartbeat_reset_txs: DashMap<String, mpsc::UnboundedSender<()>>,
    pub rng: Mutex<StdRng>, // there is only one writer, when in follower mode or in candidate mode
}

impl RaftNode {
    pub fn new(dir_path: String, rf: u32, addr: String, id: u32, peer_addrs: Vec<String>) -> Self {
        let state = RaftState::new(dir_path, id);
        let state = RwLock::new(state);
        let peer_conns = DashMap::new();
        let heartbeat_reset_txs = DashMap::new();
        let node = Self {
            state,
            // constants
            rf,
            my_addr: addr.clone(),
            my_id: id,
            peer_addrs: peer_addrs.clone(),
            // additional volatile state
            peer_conns: peer_conns,
            elec_timer_tx: Mutex::new(None),
            heartbeat_reset_txs: heartbeat_reset_txs,
            rng: Mutex::new(StdRng::seed_from_u64(id as u64)),
        };
        node
    }

    pub async fn get_election_timeout(&mut self) -> u64 {
        self.rng
            .lock()
            .await
            .random_range(MIN_ELECTION_TIMEOUT..MAX_ELECTION_TIMEOUT)
    }

    pub async fn reset_election_timer(&self) {
        let elec_tx = self.elec_timer_tx.lock().await;
        if let Some(tx) = elec_tx.as_ref() {
            let _ = tx.send(()); // send a message to reset the timer
        }
    }

    // ---------------------------------------------------------------
    // -------------------- Connection Management --------------------
    // ---------------------------------------------------------------

    pub async fn get_or_connect_client(&self, addr: String) -> Option<RaftServiceClient<Channel>> {
        // check if the conn exists
        if let Some(client) = self.peer_conns.get(&addr) {
            return Some(client.clone());
        }

        debug!("Connecting to {}", addr);
        let mut retries = 0;
        while retries < MAX_RETRIES_PER_CONN {
            let peer_addr = format!("http://{}", addr);
            match RaftServiceClient::connect(peer_addr.clone()).await {
                Ok(conn) => {
                    self.peer_conns.insert(addr.clone(), conn.clone());
                    debug!("Connected to {}", addr);
                    return Some(conn);
                }
                Err(_e) => {
                    // debug!("\tRetrying connect to {}: {}", addr, e);
                    retries += 1;
                    sleep(Duration::from_millis(RETRY_INTERVAL)).await;
                }
            }
        }
        debug!(
            "Failed to connect to {} after {} retries",
            addr, MAX_RETRIES_PER_CONN
        );
        None
    }

    pub async fn conn_to_peers(&self) {
        let peer_addrs = self.peer_addrs.clone();
        for addr in peer_addrs {
            self.get_or_connect_client(addr).await;
        }
    }
}

// ---------------------------------------------------------------
// ---------------------- Follower -------------------------------
// ---------------------- Election Timer  ------------------------
// ---------------------------------------------------------------

async fn start_election_timer(node: Arc<Mutex<RaftNode>>) {
    // the unbounded channel is a msg-passing mechanism
    // it allows async tasks to communicate without blocking
    // they do not have a limit #msgs, but can cause unbounded memory usage
    let (tx, mut rx) = mpsc::unbounded_channel::<()>();
    {
        // get lock
        let node = node.lock().await;
        let mut elec_tx = node.elec_timer_tx.lock().await;
        *elec_tx = Some(tx);
        // drop lock
    }

    loop {
        let timeout = {
            let mut node = node.lock().await;
            node.get_election_timeout().await
        };
        debug!("election after {} ms", timeout);

        let timer = sleep(Duration::from_millis(timeout));
        tokio::pin!(timer);

        tokio::select! {
            _ = &mut timer => {
                debug!("election timeout");
                // get lock
                let node = node.lock().await;
                {
                    // reset the timer channel
                    let mut elec_tx = node.elec_timer_tx.lock().await;
                    *elec_tx = None;
                }
                node.state.write().await.role = Role::Candidate;
                return; // drop lock
            }
            Some(()) = rx.recv() => {
                debug!("election reset");
            }
        }
    }
}

// ---------------------------------------------------------------
// -------------------- Candidate --------------------------------
// -------------------- Election Management ----------------------
// ---------------------------------------------------------------

async fn start_election(node: Arc<Mutex<RaftNode>>) -> bool {
    let (current_term, last_log_term, last_log_idx, my_id, rf, peer_addrs) = {
        // get lock
        let node = node.lock().await;
        let mut state = node.state.write().await;
        state.current_term += 1;
        state.voted_for = Some(node.my_id);
        state.write_config();
        (
            state.current_term,
            state.last_log_term,
            state.last_log_idx,
            node.my_id,
            node.rf,
            node.peer_addrs.clone(),
        )
        // drop lock
    };

    debug!("Started election, curr term: {}", current_term);

    // send RequestVote to all peers
    let req = RequestVoteReq {
        term: current_term,
        candidate_id: my_id,
        last_log_idx: last_log_idx,
        last_log_term: last_log_term,
    };
    let (tx, mut rx) = mpsc::channel(peer_addrs.len());
    let mut req_vote_tasks = Vec::new();
    for addr in peer_addrs.clone() {
        let addr_clone = addr.clone();
        let client = {
            // get lock
            let node_clone = Arc::clone(&node);
            let client = node_clone
                .lock()
                .await
                .get_or_connect_client(addr_clone)
                .await;
            client
            // drop lock
        };
        if client.is_none() {
            continue;
        }
        let client = client.unwrap();

        let mut client_clone = client.clone();
        let req_clone = req.clone();
        let tx_clone = tx.clone();

        let handle = tokio::spawn(async move {
            debug!("\tsend ReqVote to {}", addr);
            let resp = client_clone.request_vote(req_clone).await;
            match resp {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    debug!("\trecv ReqVoteReply from {}: {:?}", addr, resp);
                    let _ = tx_clone.send(resp).await;
                    drop(tx_clone);
                }
                Err(e) => {
                    debug!("\tfailed to send ReqVote to {}: {}", addr, e);
                }
            }
        });
        req_vote_tasks.push(handle);
    }
    drop(tx); // close the original sender so rx.recv() ends when all tx_clones drop

    {
        // check if we are still candidate
        // get lock
        let node = node.lock().await;
        let role = node.state.read().await.role.clone();
        if role != Role::Candidate {
            debug!("drop election, role changed to {:?}", role);
            return false;
        }
        // drop lock
    }

    let mut total_votes_recv = 1; // count self vote
    let start = Instant::now();
    let deadline = {
        // get lock
        let mut node = node.lock().await;
        node.get_election_timeout().await
        // drop lock
    };
    let deadline = Duration::from_millis(deadline);
    let mut updated_term = current_term;
    while start.elapsed() < deadline && total_votes_recv <= rf / 2 {
        let remaining = deadline - start.elapsed();
        match timeout(remaining, rx.recv()).await {
            Ok(Some(resp)) => {
                if resp.term > current_term {
                    updated_term = resp.term;
                    break;
                }
                if resp.vote_granted {
                    total_votes_recv += 1;
                }
            }
            Ok(None) => {
                debug!("\tvote resp channel closed early");
                break;
            }
            Err(_) => {
                debug!("\tvote resp timeout");
                break;
            }
        }
    }
    // kill all remaining tasks
    for handle in req_vote_tasks {
        handle.abort();
        let _ = handle.await;
    }

    // get lock
    let node = node.lock().await;
    let mut state = node.state.write().await;
    if total_votes_recv > rf / 2 {
        // become leader
        debug!(
            "ElecRes: LEADER, term {}, {}/{}",
            current_term, total_votes_recv, rf
        );
        eprintln!(
            "\n{}\nElection result: LEADER {} {}",
            chrono::Local::now(),
            total_votes_recv,
            rf
        );
        state.role = Role::Leader;
        return true;
    } else {
        // become follower
        debug!(
            "ElecRes: follower, term {}, {}/{}",
            current_term, total_votes_recv, rf
        );
        eprintln!(
            "\n{}\nElection result: follower {} {}",
            chrono::Local::now(),
            total_votes_recv,
            rf
        );
        if updated_term > current_term {
            state.current_term = updated_term;
            state.voted_for = None;
            state.write_config();
        }
        state.role = Role::Follower;
        return false;
    }
    // drop lock
}

// ---------------------------------------------------------------
// ---------------------- Leader ---------------------------------
// ------------------- AppendEntriesResp Mngt --------------------
// ---------------------------------------------------------------

async fn handle_append_entries_resp(
    node: Arc<Mutex<RaftNode>>,
    node_addr: String,
    req: AppendEntriesReq,
    resp: AppendEntriesResp,
    reset_timer: bool,
) -> Result<(), String> {
    debug!("handle_append_entries_resp: {:?}", resp);

    let mut sent_log_idx = req.prev_log_idx;
    let mut is_heartbeat = false;
    if req.entries.len() > 0 {
        if let Some(last_entry) = req.entries.last() {
            sent_log_idx = last_entry.idx;
            is_heartbeat = false;
        }
    }
    {
        // get lock
        let node = node.lock().await;
        let mut state = node.state.write().await;

        // step down to follower if term is greater
        if resp.term > state.current_term {
            state.current_term = resp.term;
            state.voted_for = None;
            state.write_config();
            state.role = Role::Follower;
            debug!("\tstep down to follower");
            return Ok(());
        }

        // reset heartbeat timer if needed
        if reset_timer {
            let hbeat_tx = node.heartbeat_reset_txs.get(&node_addr);
            if let Some(tx) = hbeat_tx {
                let _ = tx.send(()); // send a message to reset the timer
            }
        }

        if resp.success {
            // update match_idx and next_idx
            for (i, addr) in node.peer_addrs.iter().enumerate() {
                if addr == &node_addr {
                    {
                        let match_idx = state.match_idx.as_mut().unwrap();
                        match_idx[i] = sent_log_idx
                    }
                    {
                        let next_idx = state.next_idx.as_mut().unwrap();
                        next_idx[i] = sent_log_idx + 1;
                    }
                    break;
                }
            }

            if is_heartbeat {
                // if heartbeat, no need to update commit index
                return Ok(());
            }

            // Recalculate commit index based on majority match_idx
            let mut match_indexes = state.match_idx.as_ref().unwrap().clone();
            match_indexes.push(state.last_log_idx); // include self (leader has full log)
            match_indexes.sort();
            let majority_idx = match_indexes[match_indexes.len() / 2];

            if majority_idx > state.commit_idx {
                // Raft commit rule: only commit entries from current term
                // Prevents old leaders’ entries being committed after leadership change
                // if let Some(entry) = state.s(majority_idx) {
                //     if entry.term == state.current_term {
                //         state.commit_idx = majority_idx;
                //     }
                // }
                let current_term = state.current_term;
                let got_idx = state
                    .check_for_conflicts_fast(majority_idx, current_term)
                    .await;
                if got_idx == majority_idx {
                    state.commit_idx = got_idx;
                    info!("Commit idx updated to {}", got_idx);
                } else {
                    debug!("conflict found, got idx: {}", got_idx);
                }
            }
            return Ok(());
        }
        // if resp.success == false,
        // it means the follower's log is inconsistent
        debug!("\t{}: follower's log is inconsistent", node_addr);
        for (i, addr) in node.peer_addrs.iter().enumerate() {
            if addr == &node_addr {
                // decrement next_idx (with floor at 1)
                let next_idx = state.next_idx.as_mut().unwrap();
                let old = next_idx[i];
                // next_idx[i] = std::cmp::max(1, next_idx[i] - 1);
                // subtract 3 without overflow
                next_idx[i] = std::cmp::max(1, next_idx[i].saturating_sub(3));
                debug!(
                    "\t{}: next_idx[{}] = {} (old {})",
                    addr, i, next_idx[i], old
                );
                break;
            }
        }
        // drop lock
    }
    Ok(())
}

// ---------------------------------------------------------------
// ---------------------- Leader ---------------------------------
// ---------------------- Heartbeat Management -------------------
// ---------------------------------------------------------------

async fn send_a_heartbeat(node: Arc<Mutex<RaftNode>>, addr: String) {
    let (term, my_id, leader_commit, prev_log_idx, prev_log_term, entries) = {
        // get lock
        let node = node.lock().await;
        let state = node.state.write().await;
        debug!("prep heartbeat to {}", addr);
        state.print_state();

        let i = node.peer_addrs.iter().position(|a| a == &addr).unwrap(); // get follower index
        let next_idx = state.next_idx.as_ref().unwrap()[i];
        let prev_log_idx = next_idx - 1;
        let prev_log_term = if prev_log_idx == 0 {
            0 // no previous entry
        } else {
            state.get_term_at_index(prev_log_idx).unwrap()
        };
        let entries = state.get_log_entries_from(next_idx).await;

        (
            state.current_term,
            node.my_id,
            state.commit_idx,
            prev_log_idx,
            prev_log_term,
            entries,
        )
        // drop lock
    };

    let req = AppendEntriesReq {
        term,
        leader_id: my_id,
        prev_log_idx,
        prev_log_term,
        entries: entries,
        leader_commit,
    };
    debug!("prep req: {:?}", req);

    let client = {
        // get lock
        let node = node.lock().await;
        node.get_or_connect_client(addr.clone()).await
        // drop lock
    };

    if let Some(mut client) = client {
        if req.entries.len() > 0 {
            debug!("\t send overloaded heartbeat to {}: {:?}", addr, req);
        } else {
            debug!("\t send heartbeat to {}: {:?}", addr, req);
        }
        let result = tokio::time::timeout(
            Duration::from_millis(HEARTBEAT_RESP_TIMEOUT),
            client.append_entries(req.clone()),
        )
        .await;
        match result {
            Ok(Ok(resp)) => {
                let resp = resp.into_inner();
                debug!("\t{}: {:?}", addr, resp);
                let node = node.clone();
                let addr = addr.clone();
                let req = req.clone();
                let resp = resp.clone();
                let _ = handle_append_entries_resp(node, addr, req, resp, false).await;
            }
            Ok(Err(e)) => {
                debug!("Failed to send heartbeat to {}: {}", addr, e);
            }
            Err(_) => {
                debug!("Heartbeat to {} timed out", addr);
            }
        }
    }
}

async fn send_heartbeat_per_node(node: Arc<Mutex<RaftNode>>, addr: String) {
    // create resettable timer channel
    let (tx, mut rx) = mpsc::unbounded_channel::<()>();
    {
        // get lock
        let node = node.lock().await;
        node.heartbeat_reset_txs.insert(addr.clone(), tx);
        // drop lock
    }

    loop {
        let timer = sleep(Duration::from_millis(HEARTBEAT_INTERVAL));
        tokio::pin!(timer);

        tokio::select! {
            _ = &mut timer => {
                send_a_heartbeat(node.clone(), addr.clone()).await;
                // debug!("heartbeat sent to {}", addr);
            }
            Some(()) = rx.recv() => {
                debug!("heartbeat reset");
            }
        }
    }
}

// ---------------------------------------------------------------
// ------------------------- Common ------------------------------
// ---------------------------------------------------------------

async fn start_log_applier(node: Arc<Mutex<RaftNode>>) {
    tokio::spawn(async move {
        loop {
            {
                // get lock
                let node_guard = node.lock().await;
                let mut state = node_guard.state.write().await;
                // debug!(
                //     "Log applier: last_applied: {}, commit_idx: {}",
                //     state.last_applied, state.commit_idx
                // );
                state.apply_pending_commands();
                // drop lock
            }

            // Sleep briefly before checking again
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

// ---------------------------------------------------------------
// ---------------------- Main Loop ------------------------------
// ---------------------------------------------------------------

async fn run_as_follower(node: Arc<Mutex<RaftNode>>) {
    debug!("\nRunning as follower");
    start_election_timer(node.clone()).await; // out of this loop means election timeout, means candidate
    // role should be candidate
}

async fn run_as_candidate(node: Arc<Mutex<RaftNode>>) {
    debug!("\nRunning as Candidate");
    let _elected = start_election(node.clone()).await;
    // role should be leader or follower
}

async fn run_as_leader(node: Arc<Mutex<RaftNode>>) {
    debug!("\nRunning as LEADER");

    // initialize match_idx and next_idx
    {
        // get lock
        let node = node.lock().await;
        let mut state = node.state.write().await;
        let mut match_idx = Vec::new();
        let mut next_idx = Vec::new();
        for _ in 0..node.rf {
            match_idx.push(0);
            next_idx.push(state.last_log_idx + 1);
        }
        state.match_idx = Some(match_idx);
        state.next_idx = Some(next_idx);
        // drop lock
    }

    // Send heartbeat to all peers
    let peer_addrs = {
        // get lock
        let node = node.lock().await;
        node.peer_addrs.clone()
        // drop lock
    };
    let mut handles = Vec::new();
    for addr in peer_addrs.clone() {
        let addr_clone = addr.clone();
        let node_clone = node.clone();
        let handle = tokio::spawn(async move {
            send_heartbeat_per_node(node_clone, addr_clone).await;
        });
        handles.push(handle);
    }

    // Wait for leader role change
    loop {
        {
            // get lock
            let node = node.lock().await;
            let role = node.state.read().await.role.clone();
            if role != Role::Leader {
                debug!("Leader role changed to {:?}", role);
                break;
            }
            // drop lock
        }
        sleep(Duration::from_millis(HEARTBEAT_INTERVAL)).await;
    }

    // Not leader anymore, abort all heartbeat tasks
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
    // TODO: delete match_idx and next_idx
    debug!("Leader heartbeat loop ended");
}

async fn run_raft_node(node: Arc<Mutex<RaftNode>>) {
    {
        // get lock
        let node = node.lock().await;
        debug!("RaftNode startup");
        node.conn_to_peers().await;
        // drop lock
    }

    start_log_applier(node.clone()).await; // start log applier for every raft node

    // Start the raft state machine
    loop {
        let role = {
            // get lock
            let node = node.lock().await;
            let role = node.state.read().await.role.clone();
            role
            // drop lock
        };

        match role {
            Role::Follower => {
                run_as_follower(node.clone()).await;
            }
            Role::Candidate => {
                run_as_candidate(node.clone()).await;
            }
            Role::Leader => {
                run_as_leader(node.clone()).await;
            }
        }
    }
}

pub async fn start_raft_service(
    dir_path: String,
    rf: u32,
    addr: String,
    id: u32,
    peer_addrs: Vec<String>,
) -> Result<Arc<Mutex<RaftNode>>, Box<dyn std::error::Error>> {
    let node = RaftNode::new(dir_path, rf, addr.clone(), id, peer_addrs.clone());
    let service = MyRaftService::new(node);
    let return_node = service.node.clone();
    {
        let node = service.node.clone();
        tokio::spawn(run_raft_node(node.clone()));
    }

    // // Reset timer once after some delay // for testing
    // tokio::time::sleep(Duration::from_secs(2)).await;
    // service.reset_election_timer().await;

    {
        let addr = addr.parse::<SocketAddr>()?;
        // // extract port and start at 0.0.0.0:port
        // let port = addr.port();
        // let addr = format!("0.0.0.0:{}", port)
        //     .parse::<SocketAddr>()
        //     .expect("Invalid address");
        let svc = RaftServiceServer::new(service); // Arc is cloneable
        tokio::spawn(Server::builder().add_service(svc).serve(addr));
    }
    info!("Raft service started at {}", addr);
    Ok(return_node)
}

pub async fn raft_is_leader(node: Arc<Mutex<RaftNode>>) -> (bool, Option<u32>) {
    // get lock
    let node = node.lock().await;
    let state = node.state.read().await;
    if state.role == Role::Leader {
        return (true, None);
    }
    (false, state.current_leader)
    // drop lock
}

pub async fn raft_write(node: Arc<Mutex<RaftNode>>, command: String) -> Result<(), String> {
    debug!("raft_write: {:?}", command);

    let (current_term, my_id, peer_addrs, rf, new_cmd_idx, new_cmd_term) = {
        // get lock
        let node = node.lock().await;
        let mut state = node.state.write().await;

        // reject write if not leader
        if state.role != Role::Leader {
            info!("raft_write: not leader, role: {:?}", state.role);
            return Err("not leader".into());
        }

        let (new_idx, new_term) = state.append_to_log(command.clone());
        (
            state.current_term,
            node.my_id,
            node.peer_addrs.clone(),
            node.rf,
            new_idx,
            new_term,
        )
        // drop lock
    };

    // create AppendEntriesReq
    let entry = LogEntry {
        idx: new_cmd_idx,
        term: new_cmd_term,
        command: command.clone(),
    };
    let (prev_log_idx, prev_log_term) = {
        // get lock
        // get from in_mem_log
        let node = node.lock().await;
        let state = node.state.read().await;
        let prev_log_idx = if new_cmd_idx > 1 { new_cmd_idx - 1 } else { 0 };
        let prev_log_term = state.get_term_at_index(prev_log_idx).unwrap_or(0);
        (prev_log_idx, prev_log_term)
        // drop lock
    };

    let entries = vec![entry];
    let append_req = AppendEntriesReq {
        term: current_term,
        leader_id: my_id,
        prev_log_idx: prev_log_idx,
        prev_log_term: prev_log_term,
        entries: entries,
        leader_commit: {
            let node = node.lock().await;
            let state = node.state.read().await;
            state.commit_idx
        },
    };

    // send AppendEntries to all peers asynchronously and
    let (tx, mut rx) = mpsc::channel(peer_addrs.len());
    for addr in peer_addrs.clone() {
        let node_clone = node.clone();
        let tx_clone = tx.clone();
        let req_clone = append_req.clone();

        tokio::spawn(async move {
            let client = {
                // get lock
                let node = node_clone.lock().await;
                let client = node.get_or_connect_client(addr.clone()).await;
                client
                // drop lock
            };
            if let Some(mut client) = client {
                debug!("\tsend log append to {}", addr);
                match client.append_entries(req_clone.clone()).await {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        debug!("\trecv log append resp from {}: {:?}", addr, resp);
                        if resp.success {
                            let _ = tx_clone.send(true).await;
                        }
                        let node_clone = node_clone.clone();
                        let addr = addr.clone();
                        let req_clone = req_clone.clone();
                        let resp = resp.clone();

                        let _ = handle_append_entries_resp(node_clone, addr, req_clone, resp, true)
                            .await;
                    }
                    Err(_) => {
                        debug!("\tfailed to send log append to {}", addr);
                    }
                }
            }
        });
    }
    drop(tx); // close the sender so rx finishes when done

    // Count replies
    let mut success_count = 1; // count self
    let deadline = Duration::from_millis(LOGAPPEND_TIMEOUT);
    let start = tokio::time::Instant::now();
    while start.elapsed() < deadline && success_count <= rf / 2 {
        let remaining = deadline - start.elapsed();
        match timeout(remaining, rx.recv()).await {
            Ok(Some(true)) => {
                success_count += 1;
            }
            Ok(None) => {
                info!("raft_write_failed: channel closed early");
                break;
            }
            Ok(Some(false)) => {
                info!("raft_write_failed: false");
                break;
            }
            Err(_) => {
                info!("raft_write_failed: timeout");
                break;
            }
        }
    }

    if success_count > rf / 2 {
        // get lock
        let node = node.lock().await;
        let mut state = node.state.write().await;
        state.commit_idx = std::cmp::max(state.commit_idx, new_cmd_idx); // because new command written already
        state.apply_command(new_cmd_idx, command);
        info!(
            "raft_write: success, commit_idx: {}, last_applied: {}",
            state.commit_idx, state.last_applied
        );
        return Ok(());
        // drop lock
    }

    debug!("raft_write: failed, success_count: {}", success_count);
    Err(format!("raft_write: failed succ_count {}", success_count).into())
}

pub async fn get_raft_state_machine(
    node: Arc<Mutex<RaftNode>>,
) -> Arc<std::sync::RwLock<BTreeMap<String, String>>> {
    debug!("get_raft_state_machine");
    // get lock
    let node = node.lock().await;
    // first apply pending commands to state machine
    {
        let mut state = node.state.write().await;
        state.apply_pending_commands();
    }
    let state = node.state.read().await;
    let state_machine = state.get_state_machine();
    state_machine
    // drop lock
}
