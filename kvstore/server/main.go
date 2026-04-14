package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	kvpb "madkv/kvstore/gen/kvpb"
	_ "modernc.org/sqlite"
)

type item struct {
	key   string
	value string
}

func (a item) Less(b btree.Item) bool { return a.key < b.(item).key }

const (
	dbFileName           = "commands.db"
	requestIDMetadataKey = "x-request-id"
	roleFollower         = "follower"
	roleCandidate        = "candidate"
	roleLeader           = "leader"
	leaderLeaseInterval  = 750 * time.Millisecond
)

type cachedMutation struct {
	op          kvpb.WALCommand_Op
	key         string
	value       string
	found       bool
	oldValue    string
	hasOldValue bool
}

type applyResult struct {
	command *kvpb.ClientCommand
	cached  cachedMutation
}

type kvServer struct {
	kvpb.UnimplementedKVSServer
	kvpb.UnimplementedRaftPeerServer

	mu            sync.Mutex
	tree          *btree.BTree
	db            *sql.DB
	partitionID   int
	replicaID     int
	serverRF      int
	numPartitions int
	apiAddr       string

	peerReplicaIDs []int
	peerP2PAddrs   map[int]string
	peerClients    map[int]kvpb.RaftPeerClient
	peerConns      map[int]*grpc.ClientConn

	rng *rand.Rand

	currentTerm uint64
	votedFor    int
	role        string
	leaderID    int
	leaderAddr  string

	logEntries  []*kvpb.RaftLogEntry
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[int]uint64
	matchIndex map[int]uint64
	recentAcks map[int]time.Time

	lastContact      time.Time
	electionDeadline time.Time
	leaderLeaseUntil time.Time

	dedup   map[string]cachedMutation
	waiters map[uint64][]chan applyResult
}

func ownerForKey(key string, numPartitions int) int {
	if numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(numPartitions))
}

func parseCommaList(raw string) []string {
	if raw == "" || raw == "none" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func newKVServer(backerDir string, partitionID, replicaID, serverRF, numPartitions int, apiAddr string, peerAddrs []string) (*kvServer, error) {
	if err := os.MkdirAll(backerDir, 0o755); err != nil {
		return nil, fmt.Errorf("create backer directory: %w", err)
	}
	dbPath := filepath.Join(backerDir, dbFileName)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	peerReplicaIDs := make([]int, 0, len(peerAddrs))
	peerP2PAddrs := make(map[int]string, len(peerAddrs))
	pos := 0
	for id := 0; id < serverRF; id++ {
		if id == replicaID {
			continue
		}
		if pos >= len(peerAddrs) {
			_ = db.Close()
			return nil, fmt.Errorf("missing peer address for replica %d", id)
		}
		peerReplicaIDs = append(peerReplicaIDs, id)
		peerP2PAddrs[id] = peerAddrs[pos]
		pos++
	}
	if pos != len(peerAddrs) {
		_ = db.Close()
		return nil, fmt.Errorf("unexpected extra peer addresses")
	}

	s := &kvServer{
		tree:           btree.New(8),
		db:             db,
		partitionID:    partitionID,
		replicaID:      replicaID,
		serverRF:       serverRF,
		numPartitions:  numPartitions,
		apiAddr:        apiAddr,
		peerReplicaIDs: peerReplicaIDs,
		peerP2PAddrs:   peerP2PAddrs,
		peerClients:    make(map[int]kvpb.RaftPeerClient, len(peerAddrs)),
		peerConns:      make(map[int]*grpc.ClientConn, len(peerAddrs)),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano() + int64(replicaID*997+partitionID*7919))),
		role:           roleFollower,
		leaderID:       -1,
		votedFor:       -1,
		nextIndex:      make(map[int]uint64, serverRF),
		matchIndex:     make(map[int]uint64, serverRF),
		recentAcks:     make(map[int]time.Time, serverRF),
		dedup:          make(map[string]cachedMutation),
		waiters:        make(map[uint64][]chan applyResult),
	}
	if err := s.initDB(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.loadPersistentState(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.resetElectionDeadlineLocked()
	s.lastContact = time.Now()
	return s, nil
}

func (s *kvServer) initDB() error {
	if _, err := s.db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = FULL;
		CREATE TABLE IF NOT EXISTS raft_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS raft_log (
			log_index INTEGER PRIMARY KEY,
			term INTEGER NOT NULL,
			payload BLOB NOT NULL
		);
	`); err != nil {
		return fmt.Errorf("initialize sqlite schema: %w", err)
	}
	return nil
}

func (s *kvServer) loadPersistentState() error {
	meta := make(map[string]string)
	rows, err := s.db.Query(`SELECT key, value FROM raft_meta`)
	if err != nil {
		return fmt.Errorf("query raft_meta: %w", err)
	}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			rows.Close()
			return fmt.Errorf("scan raft_meta: %w", err)
		}
		meta[key] = value
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate raft_meta: %w", err)
	}
	rows.Close()

	if v := meta["current_term"]; v != "" {
		term, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("parse current_term: %w", err)
		}
		s.currentTerm = term
	}
	s.votedFor = -1
	if v := meta["voted_for"]; v != "" {
		votedFor, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse voted_for: %w", err)
		}
		s.votedFor = votedFor
	}
	if v := meta["commit_index"]; v != "" {
		commit, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("parse commit_index: %w", err)
		}
		s.commitIndex = commit
	}

	logRows, err := s.db.Query(`SELECT log_index, term, payload FROM raft_log ORDER BY log_index ASC`)
	if err != nil {
		return fmt.Errorf("query raft_log: %w", err)
	}
	defer logRows.Close()

	for logRows.Next() {
		var idx uint64
		var term uint64
		var payload []byte
		if err := logRows.Scan(&idx, &term, &payload); err != nil {
			return fmt.Errorf("scan raft_log row: %w", err)
		}
		var cmd kvpb.ClientCommand
		if err := proto.Unmarshal(payload, &cmd); err != nil {
			return fmt.Errorf("decode raft payload: %w", err)
		}
		s.logEntries = append(s.logEntries, &kvpb.RaftLogEntry{
			Index:   idx,
			Term:    term,
			Command: &cmd,
		})
	}
	if err := logRows.Err(); err != nil {
		return fmt.Errorf("iterate raft_log rows: %w", err)
	}
	if s.commitIndex > s.lastLogIndexLocked() {
		s.commitIndex = s.lastLogIndexLocked()
	}
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.logEntries[s.lastApplied-1]
		cached, err := s.applyEntryLocked(entry)
		if err != nil {
			return err
		}
		if entry.Command != nil && entry.Command.RequestId != "" {
			s.dedup[entry.Command.RequestId] = cached
		}
	}
	return nil
}

func (s *kvServer) persistMetaLocked(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO raft_meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("persist meta %s: %w", key, err)
	}
	return nil
}

func (s *kvServer) persistLogEntryLocked(entry *kvpb.RaftLogEntry) error {
	payload, err := proto.Marshal(entry.Command)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}
	if _, err := s.db.Exec(`INSERT INTO raft_log(log_index, term, payload) VALUES(?, ?, ?) ON CONFLICT(log_index) DO UPDATE SET term = excluded.term, payload = excluded.payload`, entry.Index, entry.Term, payload); err != nil {
		return fmt.Errorf("persist log entry %d: %w", entry.Index, err)
	}
	return nil
}

func (s *kvServer) deleteLogSuffixLocked(fromIndex uint64) error {
	if fromIndex == 0 {
		return nil
	}
	if _, err := s.db.Exec(`DELETE FROM raft_log WHERE log_index >= ?`, fromIndex); err != nil {
		return fmt.Errorf("delete log suffix from %d: %w", fromIndex, err)
	}
	if fromIndex <= uint64(len(s.logEntries)) {
		s.logEntries = s.logEntries[:fromIndex-1]
	}
	if s.commitIndex >= fromIndex {
		s.commitIndex = fromIndex - 1
		if err := s.persistMetaLocked("commit_index", strconv.FormatUint(s.commitIndex, 10)); err != nil {
			return err
		}
	}
	if s.lastApplied > s.commitIndex {
		s.lastApplied = s.commitIndex
	}
	return nil
}

func (s *kvServer) lastLogIndexLocked() uint64 {
	if len(s.logEntries) == 0 {
		return 0
	}
	return s.logEntries[len(s.logEntries)-1].Index
}

func (s *kvServer) lastLogTermLocked() uint64 {
	if len(s.logEntries) == 0 {
		return 0
	}
	return s.logEntries[len(s.logEntries)-1].Term
}

func (s *kvServer) logTermLocked(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index > uint64(len(s.logEntries)) {
		return 0
	}
	return s.logEntries[index-1].Term
}

func (s *kvServer) resetElectionDeadlineLocked() {
	timeout := time.Duration(2000+s.rng.Intn(2000)) * time.Millisecond
	s.electionDeadline = time.Now().Add(timeout)
}

func (s *kvServer) becomeFollowerLocked(term uint64, leaderID int, leaderAddr string) error {
	if term > s.currentTerm {
		s.currentTerm = term
		s.votedFor = -1
		if err := s.persistMetaLocked("current_term", strconv.FormatUint(s.currentTerm, 10)); err != nil {
			return err
		}
		if err := s.persistMetaLocked("voted_for", strconv.Itoa(s.votedFor)); err != nil {
			return err
		}
	}
	s.role = roleFollower
	s.leaderID = leaderID
	s.leaderAddr = leaderAddr
	s.lastContact = time.Now()
	s.resetElectionDeadlineLocked()
	return nil
}

func (s *kvServer) becomeLeaderLocked() {
	s.role = roleLeader
	s.leaderID = s.replicaID
	s.leaderAddr = s.apiAddr
	s.leaderLeaseUntil = time.Time{}
	clear(s.recentAcks)
	next := s.lastLogIndexLocked() + 1
	for id := 0; id < s.serverRF; id++ {
		s.nextIndex[id] = next
		s.matchIndex[id] = 0
	}
	s.matchIndex[s.replicaID] = s.lastLogIndexLocked()
	s.nextIndex[s.replicaID] = s.lastLogIndexLocked() + 1
	s.resetElectionDeadlineLocked()
	log.Printf("partition %d replica %d became leader for term %d", s.partitionID, s.replicaID, s.currentTerm)
}

func (s *kvServer) renewLeaderLeaseLocked() {
	s.leaderLeaseUntil = time.Now().Add(leaderLeaseInterval)
}

func (s *kvServer) hasLeaderLeaseLocked() bool {
	if s.serverRF <= 1 {
		return true
	}
	return time.Now().Before(s.leaderLeaseUntil)
}

func (s *kvServer) maybeRenewLeaderLeaseLocked() {
	if s.serverRF <= 1 {
		s.renewLeaderLeaseLocked()
		return
	}
	cutoff := time.Now().Add(-leaderLeaseInterval)
	acks := 1
	for _, peerID := range s.peerReplicaIDs {
		if ts, ok := s.recentAcks[peerID]; ok && !ts.Before(cutoff) {
			acks++
		}
	}
	if acks > s.serverRF/2 {
		s.renewLeaderLeaseLocked()
	}
}

func (s *kvServer) ensurePeerClient(replicaID int) (kvpb.RaftPeerClient, error) {
	if cli := s.peerClients[replicaID]; cli != nil {
		return cli, nil
	}
	conn, err := grpc.NewClient(s.peerP2PAddrs[replicaID], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	s.peerConns[replicaID] = conn
	s.peerClients[replicaID] = kvpb.NewRaftPeerClient(conn)
	return s.peerClients[replicaID], nil
}

func (s *kvServer) resetPeerClient(replicaID int) {
	if conn := s.peerConns[replicaID]; conn != nil {
		_ = conn.Close()
	}
	delete(s.peerConns, replicaID)
	delete(s.peerClients, replicaID)
}

func notLeaderError(addr string) error {
	return status.Errorf(codes.FailedPrecondition, "not leader: %s", addr)
}

func parseMutationRequestID(ctx context.Context) (string, bool, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false, nil
	}
	values := md.Get(requestIDMetadataKey)
	if len(values) == 0 {
		return "", false, nil
	}
	if len(values) != 1 {
		return "", false, status.Errorf(codes.InvalidArgument, "expected exactly one %q header", requestIDMetadataKey)
	}
	reqID := strings.TrimSpace(values[0])
	if reqID == "" {
		return "", false, status.Errorf(codes.InvalidArgument, "%q header cannot be empty", requestIDMetadataKey)
	}
	return reqID, true, nil
}

func commandsEqual(a, b *kvpb.ClientCommand) bool {
	return proto.Equal(a, b)
}

func validateCachedMutation(cached cachedMutation, wal *kvpb.WALCommand) error {
	if cached.op != wal.Op || cached.key != wal.Key || cached.value != wal.Value {
		return status.Errorf(codes.AlreadyExists, "request id reused with different operation")
	}
	return nil
}

func (s *kvServer) validateKeyOwner(key string) error {
	if ownerForKey(key, s.numPartitions) != s.partitionID {
		return status.Errorf(codes.FailedPrecondition, "wrong partition for key %q", key)
	}
	return nil
}

func (s *kvServer) applyWALLocked(wal *kvpb.WALCommand) cachedMutation {
	switch wal.Op {
	case kvpb.WALCommand_OP_PUT:
		prev := s.tree.Get(item{key: wal.Key})
		_ = s.tree.ReplaceOrInsert(item{key: wal.Key, value: wal.Value})
		return cachedMutation{op: wal.Op, key: wal.Key, value: wal.Value, found: prev != nil}
	case kvpb.WALCommand_OP_SWAP:
		prev := s.tree.Get(item{key: wal.Key})
		_ = s.tree.ReplaceOrInsert(item{key: wal.Key, value: wal.Value})
		if prev == nil {
			return cachedMutation{op: wal.Op, key: wal.Key, value: wal.Value, found: false}
		}
		return cachedMutation{op: wal.Op, key: wal.Key, value: wal.Value, found: true, oldValue: prev.(item).value, hasOldValue: true}
	case kvpb.WALCommand_OP_DELETE:
		prev := s.tree.Get(item{key: wal.Key})
		_ = s.tree.Delete(item{key: wal.Key})
		return cachedMutation{op: wal.Op, key: wal.Key, value: wal.Value, found: prev != nil}
	default:
		return cachedMutation{op: wal.Op, key: wal.Key, value: wal.Value}
	}
}

func (s *kvServer) applyEntryLocked(entry *kvpb.RaftLogEntry) (cachedMutation, error) {
	if entry.Command == nil || entry.Command.Wal == nil {
		return cachedMutation{}, fmt.Errorf("log entry %d missing command", entry.Index)
	}
	if reqID := entry.Command.RequestId; reqID != "" {
		if cached, ok := s.dedup[reqID]; ok {
			if err := validateCachedMutation(cached, entry.Command.Wal); err != nil {
				return cachedMutation{}, err
			}
			return cached, nil
		}
	}
	cached := s.applyWALLocked(entry.Command.Wal)
	if reqID := entry.Command.RequestId; reqID != "" {
		s.dedup[reqID] = cached
	}
	return cached, nil
}

func (s *kvServer) notifyWaitersLocked(index uint64, result applyResult) {
	waiters := s.waiters[index]
	delete(s.waiters, index)
	for _, ch := range waiters {
		ch <- result
		close(ch)
	}
}

func (s *kvServer) applyCommittedEntriesLocked() error {
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.logEntries[s.lastApplied-1]
		cached, err := s.applyEntryLocked(entry)
		if err != nil {
			return err
		}
		s.notifyWaitersLocked(entry.Index, applyResult{command: entry.Command, cached: cached})
	}
	return nil
}

func (s *kvServer) maybeAdvanceCommitLocked() error {
	lastIdx := s.lastLogIndexLocked()
	for idx := lastIdx; idx > s.commitIndex; idx-- {
		if s.logTermLocked(idx) != s.currentTerm {
			continue
		}
		votes := 1
		for _, peerID := range s.peerReplicaIDs {
			if s.matchIndex[peerID] >= idx {
				votes++
			}
		}
		if votes > s.serverRF/2 {
			s.commitIndex = idx
			if err := s.persistMetaLocked("commit_index", strconv.FormatUint(s.commitIndex, 10)); err != nil {
				return err
			}
			return s.applyCommittedEntriesLocked()
		}
	}
	return nil
}

func (s *kvServer) submitCommand(ctx context.Context, command *kvpb.ClientCommand) (cachedMutation, error) {
	s.mu.Lock()
	if s.role != roleLeader {
		addr := s.leaderAddr
		s.mu.Unlock()
		return cachedMutation{}, notLeaderError(addr)
	}
	if err := s.validateKeyOwner(command.Wal.Key); err != nil {
		s.mu.Unlock()
		return cachedMutation{}, err
	}
	if command.RequestId != "" {
		if cached, ok := s.dedup[command.RequestId]; ok {
			if err := validateCachedMutation(cached, command.Wal); err != nil {
				s.mu.Unlock()
				return cachedMutation{}, err
			}
			s.mu.Unlock()
			return cached, nil
		}
	}

	entry := &kvpb.RaftLogEntry{
		Index:   s.lastLogIndexLocked() + 1,
		Term:    s.currentTerm,
		Command: command,
	}
	if err := s.persistLogEntryLocked(entry); err != nil {
		s.mu.Unlock()
		return cachedMutation{}, err
	}
	s.logEntries = append(s.logEntries, entry)
	s.matchIndex[s.replicaID] = entry.Index
	s.nextIndex[s.replicaID] = entry.Index + 1
	waitCh := make(chan applyResult, 1)
	s.waiters[entry.Index] = append(s.waiters[entry.Index], waitCh)
	if err := s.maybeAdvanceCommitLocked(); err != nil {
		s.mu.Unlock()
		return cachedMutation{}, err
	}
	s.mu.Unlock()

	s.broadcastAppendEntries()

	select {
	case <-ctx.Done():
		return cachedMutation{}, ctx.Err()
	case result := <-waitCh:
		if !commandsEqual(result.command, command) {
			return cachedMutation{}, notLeaderError("")
		}
		return result.cached, nil
	}
}

func (s *kvServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateKeyOwner(req.Key); err != nil {
		return nil, err
	}
	if s.role != roleLeader {
		return nil, notLeaderError(s.leaderAddr)
	}
	if !s.hasLeaderLeaseLocked() {
		return nil, notLeaderError("")
	}
	got := s.tree.Get(item{key: req.Key})
	if got == nil {
		return &kvpb.GetReply{Found: false}, nil
	}
	it := got.(item)
	return &kvpb.GetReply{Found: true, Value: it.value}, nil
}

func (s *kvServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutReply, error) {
	reqID, _, err := parseMutationRequestID(ctx)
	if err != nil {
		return nil, err
	}
	cached, err := s.submitCommand(ctx, &kvpb.ClientCommand{
		RequestId: reqID,
		Wal:       &kvpb.WALCommand{Op: kvpb.WALCommand_OP_PUT, Key: req.Key, Value: req.Value},
	})
	if err != nil {
		return nil, err
	}
	return &kvpb.PutReply{Found: cached.found}, nil
}

func (s *kvServer) Swap(ctx context.Context, req *kvpb.SwapRequest) (*kvpb.SwapReply, error) {
	reqID, _, err := parseMutationRequestID(ctx)
	if err != nil {
		return nil, err
	}
	cached, err := s.submitCommand(ctx, &kvpb.ClientCommand{
		RequestId: reqID,
		Wal:       &kvpb.WALCommand{Op: kvpb.WALCommand_OP_SWAP, Key: req.Key, Value: req.Value},
	})
	if err != nil {
		return nil, err
	}
	if !cached.found {
		return &kvpb.SwapReply{Found: false}, nil
	}
	return &kvpb.SwapReply{Found: true, OldValue: cached.oldValue}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteReply, error) {
	reqID, _, err := parseMutationRequestID(ctx)
	if err != nil {
		return nil, err
	}
	cached, err := s.submitCommand(ctx, &kvpb.ClientCommand{
		RequestId: reqID,
		Wal:       &kvpb.WALCommand{Op: kvpb.WALCommand_OP_DELETE, Key: req.Key},
	})
	if err != nil {
		return nil, err
	}
	return &kvpb.DeleteReply{Found: cached.found}, nil
}

func (s *kvServer) Scan(ctx context.Context, req *kvpb.ScanRequest) (*kvpb.ScanReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != roleLeader {
		return nil, notLeaderError(s.leaderAddr)
	}
	if !s.hasLeaderLeaseLocked() {
		return nil, notLeaderError("")
	}
	pairs := make([]*kvpb.KVPair, 0)
	s.tree.AscendGreaterOrEqual(item{key: req.StartKey}, func(i btree.Item) bool {
		it := i.(item)
		if it.key > req.EndKey {
			return false
		}
		pairs = append(pairs, &kvpb.KVPair{Key: it.key, Value: it.value})
		return true
	})
	return &kvpb.ScanReply{Pairs: pairs}, nil
}

func (s *kvServer) RequestVote(ctx context.Context, req *kvpb.RequestVoteRequest) (*kvpb.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.currentTerm {
		return &kvpb.RequestVoteReply{Term: s.currentTerm, VoteGranted: false}, nil
	}
	if req.Term > s.currentTerm {
		if err := s.becomeFollowerLocked(req.Term, -1, ""); err != nil {
			return nil, err
		}
	}

	upToDate := req.LastLogTerm > s.lastLogTermLocked() || (req.LastLogTerm == s.lastLogTermLocked() && req.LastLogIndex >= s.lastLogIndexLocked())
	canVote := s.votedFor == -1 || s.votedFor == int(req.CandidateId)
	if canVote && upToDate {
		s.votedFor = int(req.CandidateId)
		if err := s.persistMetaLocked("voted_for", strconv.Itoa(s.votedFor)); err != nil {
			return nil, err
		}
		s.lastContact = time.Now()
		s.resetElectionDeadlineLocked()
		return &kvpb.RequestVoteReply{Term: s.currentTerm, VoteGranted: true}, nil
	}
	return &kvpb.RequestVoteReply{Term: s.currentTerm, VoteGranted: false}, nil
}

func (s *kvServer) AppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.currentTerm {
		return &kvpb.AppendEntriesReply{Term: s.currentTerm, Success: false, MatchIndex: s.lastLogIndexLocked()}, nil
	}
	if req.Term > s.currentTerm || s.role != roleFollower {
		if err := s.becomeFollowerLocked(req.Term, int(req.LeaderId), req.LeaderApiAddr); err != nil {
			return nil, err
		}
	} else {
		s.leaderID = int(req.LeaderId)
		s.leaderAddr = req.LeaderApiAddr
		s.lastContact = time.Now()
		s.resetElectionDeadlineLocked()
	}

	if req.PrevLogIndex > s.lastLogIndexLocked() || s.logTermLocked(req.PrevLogIndex) != req.PrevLogTerm {
		return &kvpb.AppendEntriesReply{Term: s.currentTerm, Success: false, MatchIndex: s.lastLogIndexLocked()}, nil
	}

	insertAt := req.PrevLogIndex + 1
	for offset, entry := range req.Entries {
		targetIndex := insertAt + uint64(offset)
		if targetIndex <= s.lastLogIndexLocked() && s.logTermLocked(targetIndex) != entry.Term {
			if err := s.deleteLogSuffixLocked(targetIndex); err != nil {
				return nil, err
			}
		}
		if targetIndex > s.lastLogIndexLocked() {
			cloned := proto.Clone(entry).(*kvpb.RaftLogEntry)
			if err := s.persistLogEntryLocked(cloned); err != nil {
				return nil, err
			}
			s.logEntries = append(s.logEntries, cloned)
		}
	}

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = req.LeaderCommit
		if s.commitIndex > s.lastLogIndexLocked() {
			s.commitIndex = s.lastLogIndexLocked()
		}
		if err := s.persistMetaLocked("commit_index", strconv.FormatUint(s.commitIndex, 10)); err != nil {
			return nil, err
		}
		if err := s.applyCommittedEntriesLocked(); err != nil {
			return nil, err
		}
	}
	return &kvpb.AppendEntriesReply{Term: s.currentTerm, Success: true, MatchIndex: s.lastLogIndexLocked()}, nil
}

func (s *kvServer) startElection() {
	s.mu.Lock()
	if s.role == roleLeader || time.Now().Before(s.electionDeadline) {
		s.mu.Unlock()
		return
	}
	s.role = roleCandidate
	s.currentTerm++
	s.votedFor = s.replicaID
	s.leaderID = -1
	s.leaderAddr = ""
	term := s.currentTerm
	lastIndex := s.lastLogIndexLocked()
	lastTerm := s.lastLogTermLocked()
	if err := s.persistMetaLocked("current_term", strconv.FormatUint(s.currentTerm, 10)); err != nil {
		s.mu.Unlock()
		log.Printf("persist current_term failed: %v", err)
		return
	}
	if err := s.persistMetaLocked("voted_for", strconv.Itoa(s.votedFor)); err != nil {
		s.mu.Unlock()
		log.Printf("persist voted_for failed: %v", err)
		return
	}
	s.resetElectionDeadlineLocked()
	s.mu.Unlock()

	votes := 1
	var voteMu sync.Mutex
	announcedLeader := false
	for _, peerID := range s.peerReplicaIDs {
		go func(peerID int) {
			client, err := s.getPeerClient(peerID)
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			resp, err := client.RequestVote(ctx, &kvpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  uint32(s.replicaID),
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				s.mu.Lock()
				s.resetPeerClient(peerID)
				s.mu.Unlock()
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if resp.Term > s.currentTerm {
				if err := s.becomeFollowerLocked(resp.Term, -1, ""); err != nil {
					log.Printf("become follower failed: %v", err)
				}
				return
			}
			if s.role != roleCandidate || s.currentTerm != term {
				return
			}
			if resp.VoteGranted {
				shouldBroadcast := false
				voteMu.Lock()
				votes++
				shouldLead := votes > s.serverRF/2
				if shouldLead && !announcedLeader {
					announcedLeader = true
					shouldBroadcast = true
				}
				voteMu.Unlock()
				if shouldLead && s.role == roleCandidate && s.currentTerm == term {
					s.becomeLeaderLocked()
					if shouldBroadcast {
						go s.broadcastAppendEntries()
					}
				}
			}
		}(peerID)
	}
}

func (s *kvServer) getPeerClient(replicaID int) (kvpb.RaftPeerClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensurePeerClient(replicaID)
}

func (s *kvServer) replicateToPeer(peerID int) {
	s.mu.Lock()
	if s.role != roleLeader {
		s.mu.Unlock()
		return
	}
	nextIdx := s.nextIndex[peerID]
	if nextIdx == 0 {
		nextIdx = s.lastLogIndexLocked() + 1
		s.nextIndex[peerID] = nextIdx
	}
	prevIdx := nextIdx - 1
	prevTerm := s.logTermLocked(prevIdx)
	entries := make([]*kvpb.RaftLogEntry, 0)
	if nextIdx > 0 && nextIdx <= s.lastLogIndexLocked() {
		for _, entry := range s.logEntries[nextIdx-1:] {
			entries = append(entries, proto.Clone(entry).(*kvpb.RaftLogEntry))
		}
	}
	req := &kvpb.AppendEntriesRequest{
		Term:          s.currentTerm,
		LeaderId:      uint32(s.replicaID),
		PrevLogIndex:  prevIdx,
		PrevLogTerm:   prevTerm,
		Entries:       entries,
		LeaderCommit:  s.commitIndex,
		LeaderApiAddr: s.apiAddr,
	}
	s.mu.Unlock()

	client, err := s.getPeerClient(peerID)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		s.mu.Lock()
		s.resetPeerClient(peerID)
		s.mu.Unlock()
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if resp.Term > s.currentTerm {
		if err := s.becomeFollowerLocked(resp.Term, -1, ""); err != nil {
			log.Printf("become follower failed: %v", err)
		}
		return
	}
	if s.role != roleLeader || req.Term != s.currentTerm {
		return
	}
	if resp.Success {
		s.matchIndex[peerID] = resp.MatchIndex
		s.nextIndex[peerID] = resp.MatchIndex + 1
		s.recentAcks[peerID] = time.Now()
		s.maybeRenewLeaderLeaseLocked()
		if err := s.maybeAdvanceCommitLocked(); err != nil {
			log.Printf("advance commit failed: %v", err)
		}
		return
	}
	if resp.MatchIndex+1 < s.nextIndex[peerID] {
		s.nextIndex[peerID] = resp.MatchIndex + 1
	} else if s.nextIndex[peerID] > 1 {
		s.nextIndex[peerID]--
	}
}

func (s *kvServer) broadcastAppendEntries() {
	for _, peerID := range s.peerReplicaIDs {
		go s.replicateToPeer(peerID)
	}
}

func (s *kvServer) electionLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.startElection()
		}
	}
}

func (s *kvServer) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			isLeader := s.role == roleLeader
			s.mu.Unlock()
			if isLeader {
				s.broadcastAppendEntries()
			}
		}
	}
}

func registerWithManagers(managerAddrs []string, partitionID, replicaID int, apiAddr string, timeout, retryInterval time.Duration) (int, int, string, error) {
	for {
		var firstResp *kvpb.RegisterServerReply
		registeredAny := false
		for _, managerAddr := range managerAddrs {
			conn, err := grpc.NewClient(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("manager dial failed (%s): %v", managerAddr, err)
				continue
			}
			c := kvpb.NewClusterManagerClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			resp, err := c.RegisterServer(ctx, &kvpb.RegisterServerRequest{
				PartitionId: uint32(partitionID),
				ReplicaId:   uint32(replicaID),
				ApiAddr:     apiAddr,
			})
			cancel()
			_ = conn.Close()
			if err != nil {
				log.Printf("manager register failed (%s): %v", managerAddr, err)
				continue
			}
			registeredAny = true
			if firstResp == nil {
				firstResp = resp
			}
		}
		if registeredAny && firstResp != nil {
			return int(firstResp.NumPartitions), int(firstResp.ServerRf), firstResp.AssignedApiAddr, nil
		}
		time.Sleep(retryInterval)
	}
}

func main() {
	partitionID := flag.Int("partition_id", 0, "partition ID")
	replicaID := flag.Int("replica_id", 0, "replica ID within the partition")
	managerAddrsRaw := flag.String("manager_addrs", "127.0.0.1:3666", "comma-separated manager ip:port list")
	apiListen := flag.String("api_listen", "0.0.0.0:3777", "ip:port for client API")
	p2pListen := flag.String("p2p_listen", "0.0.0.0:3707", "ip:port for raft peer RPC")
	peerAddrsRaw := flag.String("peer_addrs", "none", "comma-separated peer p2p addresses excluding self")
	backerDir := flag.String("backer_path", "data", "directory where durable server state is stored")
	retryInterval := flag.Duration("retry_interval", time.Second, "retry interval for manager connectivity")
	rpcTimeout := flag.Duration("timeout", 2*time.Second, "timeout for manager RPC")
	flag.Parse()

	managerAddrs := parseCommaList(*managerAddrsRaw)
	if len(managerAddrs) == 0 {
		log.Fatalf("manager_addrs must not be empty")
	}
	peerAddrs := parseCommaList(*peerAddrsRaw)

	numPartitions, serverRF, assignedAPIAddr, err := registerWithManagers(managerAddrs, *partitionID, *replicaID, *apiListen, *rpcTimeout, *retryInterval)
	if err != nil {
		log.Fatalf("manager registration failed: %v", err)
	}
	if numPartitions <= 0 {
		log.Fatalf("invalid num partitions from manager: %d", numPartitions)
	}
	if serverRF <= 0 {
		log.Fatalf("invalid server rf from manager: %d", serverRF)
	}
	if *replicaID < 0 || *replicaID >= serverRF {
		log.Fatalf("replica id %d out of range [0,%d)", *replicaID, serverRF)
	}
	if len(peerAddrs) != serverRF-1 {
		log.Fatalf("expected %d peer addresses, got %d", serverRF-1, len(peerAddrs))
	}
	if assignedAPIAddr == "" {
		assignedAPIAddr = *apiListen
	}

	srv, err := newKVServer(*backerDir, *partitionID, *replicaID, serverRF, numPartitions, assignedAPIAddr, peerAddrs)
	if err != nil {
		log.Fatalf("server init failed: %v", err)
	}
	defer func() {
		srv.mu.Lock()
		for peerID := range srv.peerConns {
			srv.resetPeerClient(peerID)
		}
		srv.mu.Unlock()
		if err := srv.db.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			log.Printf("db close failed: %v", err)
		}
	}()

	apiLis, err := net.Listen("tcp", *apiListen)
	if err != nil {
		log.Fatalf("api listen failed: %v", err)
	}
	p2pLis, err := net.Listen("tcp", *p2pListen)
	if err != nil {
		log.Fatalf("p2p listen failed: %v", err)
	}

	apiServer := grpc.NewServer()
	kvpb.RegisterKVSServer(apiServer, srv)
	p2pServer := grpc.NewServer()
	kvpb.RegisterRaftPeerServer(p2pServer, srv)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.electionLoop(runCtx)
	go srv.heartbeatLoop(runCtx)

	go func() {
		if err := p2pServer.Serve(p2pLis); err != nil {
			log.Fatalf("p2p serve failed: %v", err)
		}
	}()

	fmt.Printf("server partition=%d replica=%d api=%s p2p=%s rf=%d\n", *partitionID, *replicaID, *apiListen, *p2pListen, serverRF)
	if err := apiServer.Serve(apiLis); err != nil {
		log.Fatalf("api serve failed: %v", err)
	}
}
