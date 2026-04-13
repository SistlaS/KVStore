package main

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	kvpb "madkv/kvstore/gen/kvpb"
)

func newTestServer(t *testing.T, backerDir string, partitionID, replicaID, serverRF, numPartitions int) *kvServer {
	t.Helper()
	peerAddrs := make([]string, 0, max(serverRF-1, 0))
	for id := 0; id < serverRF; id++ {
		if id == replicaID {
			continue
		}
		peerAddrs = append(peerAddrs, fmt.Sprintf("127.0.0.1:%d", 4700+id))
	}
	srv, err := newKVServer(backerDir, partitionID, replicaID, serverRF, numPartitions, "127.0.0.1:0", peerAddrs)
	if err != nil {
		t.Fatalf("newKVServer() failed: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.db.Close()
	})
	return srv
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func becomeTestLeader(t *testing.T, srv *kvServer, term uint64) {
	t.Helper()
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.currentTerm = term
	if err := srv.persistMetaLocked("current_term", strconv.FormatUint(term, 10)); err != nil {
		t.Fatalf("persist current_term: %v", err)
	}
	srv.becomeLeaderLocked()
}

func TestSingleReplicaLeaderCommitsAndReplays(t *testing.T) {
	backerDir := t.TempDir()
	srv := newTestServer(t, backerDir, 0, 0, 1, 1)
	becomeTestLeader(t, srv, 1)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(requestIDMetadataKey, "req-1"))
	putResp, err := srv.Put(ctx, &kvpb.PutRequest{Key: "alpha", Value: "one"})
	if err != nil {
		t.Fatalf("Put() failed: %v", err)
	}
	if putResp.Found {
		t.Fatalf("Put().Found = true, want false")
	}

	getResp, err := srv.Get(context.Background(), &kvpb.GetRequest{Key: "alpha"})
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if !getResp.Found || getResp.Value != "one" {
		t.Fatalf("Get() = %+v, want found=true value=one", getResp)
	}

	srv.mu.Lock()
	if srv.commitIndex != 1 || srv.lastApplied != 1 {
		t.Fatalf("commitIndex=%d lastApplied=%d, want 1/1", srv.commitIndex, srv.lastApplied)
	}
	srv.mu.Unlock()

	if err := srv.db.Close(); err != nil {
		t.Fatalf("close first db: %v", err)
	}

	reloaded, err := newKVServer(backerDir, 0, 0, 1, 1, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("reload newKVServer() failed: %v", err)
	}
	defer reloaded.db.Close()
	reloaded.mu.Lock()
	reloaded.role = roleLeader
	reloaded.leaderAddr = reloaded.apiAddr
	reloaded.mu.Unlock()

	got, err := reloaded.Get(context.Background(), &kvpb.GetRequest{Key: "alpha"})
	if err != nil {
		t.Fatalf("Get() after reload failed: %v", err)
	}
	if !got.Found || got.Value != "one" {
		t.Fatalf("Get() after reload = %+v, want found=true value=one", got)
	}
}

func TestFollowerRejectsClientRequestsWithLeaderHint(t *testing.T) {
	srv := newTestServer(t, t.TempDir(), 0, 0, 1, 1)
	srv.mu.Lock()
	srv.role = roleFollower
	srv.leaderAddr = "127.0.0.1:3779"
	srv.mu.Unlock()

	_, err := srv.Get(context.Background(), &kvpb.GetRequest{Key: "alpha"})
	if err == nil {
		t.Fatalf("Get() on follower unexpectedly succeeded")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if st.Message() != "not leader: 127.0.0.1:3779" {
		t.Fatalf("unexpected redirect message: %q", st.Message())
	}
}

func TestLeaderReadRequiresFreshQuorumLease(t *testing.T) {
	srv := newTestServer(t, t.TempDir(), 0, 0, 3, 1)
	becomeTestLeader(t, srv, 2)

	srv.mu.Lock()
	_ = srv.tree.ReplaceOrInsert(item{key: "alpha", value: "one"})
	srv.leaderLeaseUntil = time.Now().Add(-time.Second)
	srv.mu.Unlock()

	_, err := srv.Get(context.Background(), &kvpb.GetRequest{Key: "alpha"})
	if err == nil {
		t.Fatalf("Get() on stale leader unexpectedly succeeded")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}

	srv.mu.Lock()
	srv.renewLeaderLeaseLocked()
	srv.mu.Unlock()

	resp, err := srv.Get(context.Background(), &kvpb.GetRequest{Key: "alpha"})
	if err != nil {
		t.Fatalf("Get() with fresh lease failed: %v", err)
	}
	if !resp.Found || resp.Value != "one" {
		t.Fatalf("Get() = %+v, want found=true value=one", resp)
	}
}

func TestRequestVoteRejectsStaleCandidateLog(t *testing.T) {
	srv := newTestServer(t, t.TempDir(), 0, 0, 1, 1)
	srv.mu.Lock()
	srv.currentTerm = 3
	if err := srv.persistMetaLocked("current_term", "3"); err != nil {
		t.Fatalf("persist current_term: %v", err)
	}
	entry := &kvpb.RaftLogEntry{
		Index: 1,
		Term:  3,
		Command: &kvpb.ClientCommand{
			RequestId: "seed",
			Wal:       &kvpb.WALCommand{Op: kvpb.WALCommand_OP_PUT, Key: "k", Value: "v"},
		},
	}
	if err := srv.persistLogEntryLocked(entry); err != nil {
		t.Fatalf("persistLogEntryLocked() failed: %v", err)
	}
	srv.logEntries = append(srv.logEntries, entry)
	srv.mu.Unlock()

	resp, err := srv.RequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:         3,
		CandidateId:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("RequestVote() failed: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("VoteGranted = true, want false for stale candidate log")
	}
}

func TestAppendEntriesAppliesCommittedCommand(t *testing.T) {
	srv := newTestServer(t, t.TempDir(), 0, 1, 3, 1)

	resp, err := srv.AppendEntries(context.Background(), &kvpb.AppendEntriesRequest{
		Term:          4,
		LeaderId:      0,
		PrevLogIndex:  0,
		PrevLogTerm:   0,
		LeaderCommit:  1,
		LeaderApiAddr: "127.0.0.1:3777",
		Entries: []*kvpb.RaftLogEntry{
			{
				Index: 1,
				Term:  4,
				Command: &kvpb.ClientCommand{
					RequestId: "req-append",
					Wal:       &kvpb.WALCommand{Op: kvpb.WALCommand_OP_PUT, Key: "beta", Value: "two"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AppendEntries() failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("AppendEntries().Success = false, want true")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.commitIndex != 1 || srv.lastApplied != 1 {
		t.Fatalf("commitIndex=%d lastApplied=%d, want 1/1", srv.commitIndex, srv.lastApplied)
	}
	got := srv.tree.Get(item{key: "beta"})
	if got == nil || got.(item).value != "two" {
		t.Fatalf("applied tree value = %v, want beta=two", got)
	}
}

func TestPartitionOwnershipEnforced(t *testing.T) {
	srv := newTestServer(t, t.TempDir(), 0, 0, 1, 2)
	becomeTestLeader(t, srv, 1)

	wrongKey := "k0"
	for i := 0; i < 10000 && ownerForKey(wrongKey, 2) == 0; i++ {
		wrongKey = fmt.Sprintf("wrong-%d", i+1)
	}
	if ownerForKey(wrongKey, 2) == 0 {
		t.Fatalf("failed to find a key owned by partition 1")
	}
	_, err := srv.Put(context.Background(), &kvpb.PutRequest{Key: wrongKey, Value: "v"})
	if err == nil {
		t.Fatalf("Put() on wrong partition unexpectedly succeeded")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}
