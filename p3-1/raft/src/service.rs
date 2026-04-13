#![allow(unused)]

use crate::node::RaftNode;
use crate::raft_rpc::raft_service_server::RaftService;
use crate::raft_rpc::{AppendEntriesReq, AppendEntriesResp, RequestVoteReq, RequestVoteResp};
use crate::state::Role;
use log::{debug, info};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MyRaftService {
    pub node: Arc<Mutex<RaftNode>>,
}

impl MyRaftService {
    pub fn new(node: RaftNode) -> Self {
        let node = Arc::new(Mutex::new(node));
        Self { node }
    }
}

#[tonic::async_trait]
impl RaftService for MyRaftService {
    // -------------------------------------------------------
    // -------------------- AppendEntries --------------------
    // -------------------------------------------------------
    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesReq>,
    ) -> Result<tonic::Response<AppendEntriesResp>, tonic::Status> {
        let req = request.into_inner();
        // debug!("got {:?}", req);
        if req.entries.len() > 0 {
            debug!("got {:?}", req);
        } else {
            debug!("got heartbeat: {:?}", req);
        }
        let (current_term, last_log_idx) = {
            // get lock
            let node: tokio::sync::MutexGuard<'_, RaftNode> = self.node.lock().await;
            let mut state = node.state.write().await;
            debug!("\tstate: {:?}", state);

            // 1. Reply false if term < currentTerm (§5.1)
            if req.term < state.current_term {
                // stale
                return Ok(tonic::Response::new(AppendEntriesResp {
                    term: state.current_term,
                    good_log_idx: state.last_log_idx,
                    success: false,
                }));
            }

            // Reset election timer
            debug!("\tresetting etimer");
            node.reset_election_timer().await; // reset the election timer only if term is valid

            // if term > currentTerm, update currentTerm and convert to follower
            if req.term > state.current_term {
                // become follower
                state.current_term = req.term;
                state.voted_for = None;
                // if role is not follower, step down to follower
                if state.role != Role::Follower {
                    debug!("\tstep down {:?} --> follower", state.role);
                    state.role = Role::Follower;
                }
                state.write_config();
            }

            // update leader
            if state.current_leader != Some(req.leader_id) {
                state.current_leader = Some(req.leader_id);
            }

            // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
            if req.prev_log_idx > state.last_log_idx {
                // we are missing entries
                debug!("\tmissing entries: leader {} > my {}", req.prev_log_idx, state.last_log_idx);
                return Ok(tonic::Response::new(AppendEntriesResp {
                    term: state.current_term,
                    success: false,
                    good_log_idx: state.last_log_idx,
                }));
            }

            // 3. If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it
            if (req.prev_log_idx == state.last_log_idx)
                && (req.prev_log_term != state.last_log_term)
            {
                // we have mismatch in term
                return Ok(tonic::Response::new(AppendEntriesResp {
                    term: state.current_term,
                    good_log_idx: state.last_log_idx - 1,
                    success: false,
                }));
            }

            // check for conflicts, e.g., leader's last log index is less than us
            let good_log_idx = state
                .check_for_conflicts_fast(req.prev_log_idx, req.prev_log_term)
                .await;
            if good_log_idx != req.prev_log_idx {
                // we have mismatch in term
                return Ok(tonic::Response::new(AppendEntriesResp {
                    term: state.current_term,
                    good_log_idx,
                    success: false,
                }));
            }

            if req.entries.len() > 0 {
                debug!(
                    "my last log idx: {}, last log term {}",
                    state.last_log_idx, state.last_log_term
                );
                info!("Append to log: {:?}", req.entries);
                // 4. Append any new entries not already in the log
                state.append_to_log_multi(req.entries);
            }

            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if req.leader_commit > state.commit_idx {
                state.commit_idx = std::cmp::min(req.leader_commit, state.last_log_idx);
                debug!("\tupdated commit_idx: {}", state.commit_idx);
            }

            debug!("\taccept : {}", state.current_term);
            (state.current_term, state.last_log_idx)
            // drop lock
        };

        // Send success response
        let resp = AppendEntriesResp {
            term: current_term,
            good_log_idx: last_log_idx,
            success: true,
        };
        Ok(tonic::Response::new(resp))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<RequestVoteReq>,
    ) -> Result<tonic::Response<RequestVoteResp>, tonic::Status> {
        debug!("got {:?}", request);
        let req = request.into_inner();
        // Receiver implementation:

        let current_term = {
            // get lock
            let node = self.node.lock().await;
            let mut state = node.state.write().await;

            // 1. Reply false if term < currentTerm
            if req.term < state.current_term {
                return Ok(tonic::Response::new(RequestVoteResp {
                    term: state.current_term,
                    vote_granted: false,
                }));
            }

            // if term > currentTerm, update currentTerm and convert to follower
            if req.term > state.current_term {
                state.current_term = req.term;
                state.voted_for = None;
                state.write_config();
                if state.role != Role::Follower {
                    debug!("\tstep down {:?} --> follower", state.role);
                    state.role = Role::Follower;
                }
            }

            // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
            let can_vote = state.voted_for.is_none() || state.voted_for == Some(req.candidate_id);
            if !can_vote {
                debug!("\tno vote, already voted for {}", state.voted_for.unwrap());
                return Ok(tonic::Response::new(RequestVoteResp {
                    term: state.current_term,
                    vote_granted: false,
                }));
            }
            let log_is_uptodate = {
                if req.last_log_term > state.last_log_term {
                    true
                } else if req.last_log_term == state.last_log_term {
                    req.last_log_idx >= state.last_log_idx
                } else {
                    false
                }
            };
            if !log_is_uptodate {
                debug!("\tno vote, log not up to date");
                return Ok(tonic::Response::new(RequestVoteResp {
                    term: state.current_term,
                    vote_granted: false,
                }));
            }

            // vote for candidate
            state.voted_for = Some(req.candidate_id);
            state.write_config();
            debug!("vote yes {}", req.candidate_id);

            node.reset_election_timer().await; // reset the election timer only if vote is granted

            state.current_term
            // drop lock
        };
        let resp = RequestVoteResp {
            term: current_term,
            vote_granted: true,
        };
        Ok(tonic::Response::new(resp))
    }
}
