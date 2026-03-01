package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/btree"
	"google.golang.org/grpc"
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
	mu   sync.Mutex
	tree *btree.BTree
	db   *sql.DB
}

const dbFileName = "commands.db"

func newKVServer(backerDir string) (*kvServer, error) {
	if err := os.MkdirAll(backerDir, 0o755); err != nil {
		return nil, fmt.Errorf("create backer directory: %w", err)
	}
	dbPath := filepath.Join(backerDir, dbFileName)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	s := &kvServer{
		tree: btree.New(8),
		db:   db,
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

func (s *kvServer) applyCommand(cmd *kvpb.WALCommand) {
	switch cmd.Op {
	case kvpb.WALCommand_OP_PUT, kvpb.WALCommand_OP_SWAP:
		_ = s.tree.ReplaceOrInsert(item{key: cmd.Key, value: cmd.Value})
	case kvpb.WALCommand_OP_DELETE:
		_ = s.tree.Delete(item{key: cmd.Key})
	}
}

func (s *kvServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.tree.Get(item{key: req.Key})
	cmd := &kvpb.WALCommand{
		Op:    kvpb.WALCommand_OP_PUT,
		Key:   req.Key,
		Value: req.Value,
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

	got := s.tree.Get(item{key: req.Key})
	cmd := &kvpb.WALCommand{
		Op:    kvpb.WALCommand_OP_SWAP,
		Key:   req.Key,
		Value: req.Value,
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

	del := s.tree.Get(item{key: req.Key})
	cmd := &kvpb.WALCommand{
		Op:  kvpb.WALCommand_OP_DELETE,
		Key: req.Key,
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
	startKey := req.StartKey
	endKey := req.EndKey

	s.tree.AscendGreaterOrEqual(item{key: startKey}, func(i btree.Item) bool {
		it := i.(item)
		if it.key > endKey { // inclusive end
			return false
		}
		pairs = append(pairs, &kvpb.KVPair{Key: it.key, Value: it.value})
		return true
	})

	return &kvpb.ScanReply{Pairs: pairs}, nil
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:50051", "ip:port to listen on")
	backerDir := flag.String("backer", "data", "directory where durable server state is stored")
	flag.Parse()

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	srv, err := newKVServer(*backerDir)
	if err != nil {
		log.Fatalf("server init failed: %v", err)
	}
	defer func() {
		if err := srv.db.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			log.Printf("db close failed: %v", err)
		}
	}()

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVSServer(grpcServer, srv)

	fmt.Printf("server listening on %s with backer dir %s\n", *listenAddr, *backerDir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
