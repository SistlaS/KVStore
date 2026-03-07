package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"path/filepath"
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

type kvServer struct {
	kvpb.UnimplementedKVSServer
	mu         sync.Mutex
	tree       *btree.BTree
	db         *sql.DB
	serverID   int
	numServers int
}

const dbFileName = "commands.db"
const requestIDMetadataKey = "x-request-id"

func ownerForKey(key string, numServers int) int {
	if numServers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(numServers))
}

func newKVServer(backerDir string) (*kvServer, error) {
	return newKVServerWithPartition(backerDir, 0, 1)
}

func newKVServerWithPartition(backerDir string, serverID, numServers int) (*kvServer, error) {
	if err := os.MkdirAll(backerDir, 0o755); err != nil {
		return nil, fmt.Errorf("create backer directory: %w", err)
	}
	dbPath := filepath.Join(backerDir, dbFileName)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	s := &kvServer{
		tree:       btree.New(8),
		db:         db,
		serverID:   serverID,
		numServers: numServers,
	}
	if err := s.initDB(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.replayLog(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *kvServer) initDB() error {
	if _, err := s.db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = FULL;
		CREATE TABLE IF NOT EXISTS wal_log (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			payload BLOB NOT NULL
		);
		CREATE TABLE IF NOT EXISTS request_dedup (
			request_id TEXT PRIMARY KEY,
			op INTEGER NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			found INTEGER NOT NULL,
			old_value TEXT,
			has_old_value INTEGER NOT NULL
		);
	`); err != nil {
		return fmt.Errorf("initialize sqlite schema: %w", err)
	}
	return nil
}

func isValidOp(op kvpb.WALCommand_Op) bool {
	return op == kvpb.WALCommand_OP_PUT ||
		op == kvpb.WALCommand_OP_SWAP ||
		op == kvpb.WALCommand_OP_DELETE
}

func (s *kvServer) replayLog() error {
	rows, err := s.db.Query(`SELECT payload FROM wal_log ORDER BY seq ASC`)
	if err != nil {
		return fmt.Errorf("query wal log: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return fmt.Errorf("scan wal row: %w", err)
		}
		var cmd kvpb.WALCommand
		if err := proto.Unmarshal(payload, &cmd); err != nil {
			return fmt.Errorf("decode wal payload: %w", err)
		}
		if !isValidOp(cmd.Op) {
			return fmt.Errorf("unknown wal op: %q", cmd.Op)
		}
		s.applyCommand(&cmd)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate wal rows: %w", err)
	}
	return nil
}

func (s *kvServer) appendLog(cmd *kvpb.WALCommand) error {
	if !isValidOp(cmd.Op) {
		return fmt.Errorf("unknown wal op: %q", cmd.Op)
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("encode wal payload: %w", err)
	}
	if _, err := s.db.Exec(`INSERT INTO wal_log(payload) VALUES(?)`, payload); err != nil {
		return fmt.Errorf("append wal row: %w", err)
	}
	return nil
}

func appendLogTx(tx *sql.Tx, cmd *kvpb.WALCommand) error {
	if !isValidOp(cmd.Op) {
		return fmt.Errorf("unknown wal op: %q", cmd.Op)
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("encode wal payload: %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO wal_log(payload) VALUES(?)`, payload); err != nil {
		return fmt.Errorf("append wal row: %w", err)
	}
	return nil
}

type cachedMutation struct {
	op          kvpb.WALCommand_Op
	key         string
	value       string
	found       bool
	oldValue    string
	hasOldValue bool
}

func boolAsInt(v bool) int {
	if v {
		return 1
	}
	return 0
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

func (s *kvServer) lookupCachedMutation(reqID string) (*cachedMutation, error) {
	row := s.db.QueryRow(`
		SELECT op, key, value, found, old_value, has_old_value
		FROM request_dedup
		WHERE request_id = ?
	`, reqID)
	var op int64
	var key, value string
	var foundInt, hasOldValueInt int
	var oldValue sql.NullString
	if err := row.Scan(&op, &key, &value, &foundInt, &oldValue, &hasOldValueInt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("read request dedup row: %w", err)
	}
	return &cachedMutation{
		op:          kvpb.WALCommand_Op(op),
		key:         key,
		value:       value,
		found:       foundInt != 0,
		oldValue:    oldValue.String,
		hasOldValue: hasOldValueInt != 0,
	}, nil
}

func validateCachedMutation(cached *cachedMutation, cmd *kvpb.WALCommand) error {
	if cached.op != cmd.Op || cached.key != cmd.Key || cached.value != cmd.Value {
		return status.Errorf(codes.AlreadyExists, "request id reused with different operation")
	}
	return nil
}

func cacheMutationTx(tx *sql.Tx, reqID string, cached *cachedMutation) error {
	var oldValue interface{}
	if cached.hasOldValue {
		oldValue = cached.oldValue
	}
	if _, err := tx.Exec(`
		INSERT INTO request_dedup(request_id, op, key, value, found, old_value, has_old_value)
		VALUES(?, ?, ?, ?, ?, ?, ?)
	`, reqID, int(cached.op), cached.key, cached.value, boolAsInt(cached.found), oldValue, boolAsInt(cached.hasOldValue)); err != nil {
		return fmt.Errorf("insert request dedup row: %w", err)
	}
	return nil
}

func (s *kvServer) applyMutatingCommandWithDedup(cmd *kvpb.WALCommand, reqID string, found bool, oldValue string, hasOldValue bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin sqlite tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := appendLogTx(tx, cmd); err != nil {
		return err
	}
	if err := cacheMutationTx(tx, reqID, &cachedMutation{
		op:          cmd.Op,
		key:         cmd.Key,
		value:       cmd.Value,
		found:       found,
		oldValue:    oldValue,
		hasOldValue: hasOldValue,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite tx: %w", err)
	}
	committed = true
	s.applyCommand(cmd)
	return nil
}

func (s *kvServer) applyCommand(cmd *kvpb.WALCommand) {
	switch cmd.Op {
	case kvpb.WALCommand_OP_PUT, kvpb.WALCommand_OP_SWAP:
		_ = s.tree.ReplaceOrInsert(item{key: cmd.Key, value: cmd.Value})
	case kvpb.WALCommand_OP_DELETE:
		_ = s.tree.Delete(item{key: cmd.Key})
	}
}

func (s *kvServer) validateKeyOwner(key string) error {
	if ownerForKey(key, s.numServers) != s.serverID {
		return status.Errorf(codes.FailedPrecondition, "wrong partition for key %q", key)
	}
	return nil
}

func (s *kvServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateKeyOwner(req.Key); err != nil {
		return nil, err
	}
	reqID, hasReqID, err := parseMutationRequestID(ctx)
	if err != nil {
		return nil, err
	}
	prev := s.tree.Get(item{key: req.Key})
	cmd := &kvpb.WALCommand{Op: kvpb.WALCommand_OP_PUT, Key: req.Key, Value: req.Value}
	if hasReqID {
		cached, err := s.lookupCachedMutation(reqID)
		if err != nil {
			return nil, err
		}
		if cached != nil {
			if err := validateCachedMutation(cached, cmd); err != nil {
				return nil, err
			}
			return &kvpb.PutReply{Found: cached.found}, nil
		}
		if err := s.applyMutatingCommandWithDedup(cmd, reqID, prev != nil, "", false); err != nil {
			return nil, err
		}
		return &kvpb.PutReply{Found: prev != nil}, nil
	}
	if err := s.appendLog(cmd); err != nil {
		return nil, err
	}
	s.applyCommand(cmd)
	return &kvpb.PutReply{Found: prev != nil}, nil
}

func (s *kvServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateKeyOwner(req.Key); err != nil {
		return nil, err
	}
	got := s.tree.Get(item{key: req.Key})
	if got == nil {
		return &kvpb.GetReply{Found: false}, nil
	}
	it := got.(item)
	return &kvpb.GetReply{Found: true, Value: it.value}, nil
}

func (s *kvServer) Swap(ctx context.Context, req *kvpb.SwapRequest) (*kvpb.SwapReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateKeyOwner(req.Key); err != nil {
		return nil, err
	}
	reqID, hasReqID, err := parseMutationRequestID(ctx)
	if err != nil {
		return nil, err
	}
	got := s.tree.Get(item{key: req.Key})
	cmd := &kvpb.WALCommand{Op: kvpb.WALCommand_OP_SWAP, Key: req.Key, Value: req.Value}
	if hasReqID {
		cached, err := s.lookupCachedMutation(reqID)
		if err != nil {
			return nil, err
		}
		if cached != nil {
			if err := validateCachedMutation(cached, cmd); err != nil {
				return nil, err
			}
			if !cached.found {
				return &kvpb.SwapReply{Found: false}, nil
			}
			return &kvpb.SwapReply{Found: true, OldValue: cached.oldValue}, nil
		}
		oldValue := ""
		hasOldValue := false
		if got != nil {
			oldValue = got.(item).value
			hasOldValue = true
		}
		if err := s.applyMutatingCommandWithDedup(cmd, reqID, got != nil, oldValue, hasOldValue); err != nil {
			return nil, err
		}
		if got == nil {
			return &kvpb.SwapReply{Found: false}, nil
		}
		return &kvpb.SwapReply{Found: true, OldValue: oldValue}, nil
	}
	if err := s.appendLog(cmd); err != nil {
		return nil, err
	}
	s.applyCommand(cmd)

	if got == nil {
		return &kvpb.SwapReply{Found: false}, nil
	}
	return &kvpb.SwapReply{Found: true, OldValue: got.(item).value}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateKeyOwner(req.Key); err != nil {
		return nil, err
	}
	reqID, hasReqID, err := parseMutationRequestID(ctx)
	if err != nil {
		return nil, err
	}
	del := s.tree.Get(item{key: req.Key})
	cmd := &kvpb.WALCommand{Op: kvpb.WALCommand_OP_DELETE, Key: req.Key}
	if hasReqID {
		cached, err := s.lookupCachedMutation(reqID)
		if err != nil {
			return nil, err
		}
		if cached != nil {
			if err := validateCachedMutation(cached, cmd); err != nil {
				return nil, err
			}
			return &kvpb.DeleteReply{Found: cached.found}, nil
		}
		if err := s.applyMutatingCommandWithDedup(cmd, reqID, del != nil, "", false); err != nil {
			return nil, err
		}
		return &kvpb.DeleteReply{Found: del != nil}, nil
	}
	if err := s.appendLog(cmd); err != nil {
		return nil, err
	}
	s.applyCommand(cmd)
	return &kvpb.DeleteReply{Found: del != nil}, nil
}

func (s *kvServer) Scan(ctx context.Context, req *kvpb.ScanRequest) (*kvpb.ScanReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

func registerWithManager(managerAddr, listenAddr string, serverID int, timeout, retryInterval time.Duration) (int, error) {
	for {
		conn, err := grpc.NewClient(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("manager dial failed: %v", err)
			time.Sleep(retryInterval)
			continue
		}
		c := kvpb.NewClusterManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := c.RegisterServer(ctx, &kvpb.RegisterServerRequest{
			ServerId: uint32(serverID),
			ApiAddr:  listenAddr,
		})
		cancel()
		_ = conn.Close()
		if err != nil {
			log.Printf("manager register failed: %v", err)
			time.Sleep(retryInterval)
			continue
		}
		return int(resp.NumServers), nil
	}
}

func main() {
	listenAddr := flag.String("listen", "0.0.0.0:3777", "ip:port to listen on")
	managerAddr := flag.String("manager", "127.0.0.1:3666", "manager ip:port")
	serverID := flag.Int("id", 0, "server ID")
	backerDir := flag.String("backer", "data", "directory where durable server state is stored")
	retryInterval := flag.Duration("retry_interval", time.Second, "retry interval for manager connectivity")
	rpcTimeout := flag.Duration("timeout", 2*time.Second, "timeout for manager RPC")
	flag.Parse()

	numServers, err := registerWithManager(*managerAddr, *listenAddr, *serverID, *rpcTimeout, *retryInterval)
	if err != nil {
		log.Fatalf("manager registration failed: %v", err)
	}
	if numServers <= 0 {
		log.Fatalf("invalid num servers from manager: %d", numServers)
	}
	if *serverID < 0 || *serverID >= numServers {
		log.Fatalf("server id %d out of range [0,%d)", *serverID, numServers)
	}

	srv, err := newKVServerWithPartition(*backerDir, *serverID, numServers)
	if err != nil {
		log.Fatalf("server init failed: %v", err)
	}
	defer func() {
		if err := srv.db.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			log.Printf("db close failed: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVSServer(grpcServer, srv)

	fmt.Printf("server listening on %s (id=%d/%d) with backer dir %s\n", *listenAddr, *serverID, numServers, *backerDir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
