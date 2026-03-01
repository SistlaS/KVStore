package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	kvpb "madkv/kvstore/gen/kvpb"

	"github.com/google/btree"
	"google.golang.org/grpc"
)

type item struct {
	key   string
	value string
}

func (a item) Less(b btree.Item) bool { return a.key < b.(item).key }

type kvServer struct {
	kvpb.UnimplementedKVSServer
	mu      sync.Mutex
	tree    *btree.BTree
	logFile *os.File
}

type walCommand struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

const maxWALFrameSize = 1 << 10 // 1 KB safety bound for one command record.

func newKVServer(logPath string) (*kvServer, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	s := &kvServer{
		tree:    btree.New(8),
		logFile: f,
	}
	if err := s.replayWAL(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek wal end: %w", err)
	}
	return s, nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func readFullWithEOFAsBoundary(r io.Reader, data []byte) error {
	n, err := io.ReadFull(r, data)
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		if n == 0 {
			return io.EOF
		}
		return io.ErrUnexpectedEOF
	}
	return err
}

func isValidOp(op string) bool {
	return op == "put" || op == "swap" || op == "delete"
}

func (s *kvServer) replayWAL() error {
	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal start: %w", err)
	}

	lenBuf := make([]byte, 4)
	for {
		err := readFullWithEOFAsBoundary(s.logFile, lenBuf)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Ignore a trailing partial frame (e.g., crash mid-write).
			return nil
		}
		if err != nil {
			return fmt.Errorf("read wal frame length: %w", err)
		}

		sz := binary.BigEndian.Uint32(lenBuf)
		if sz > maxWALFrameSize {
			return fmt.Errorf("wal frame too large: %d bytes", sz)
		}
		payload := make([]byte, sz)
		err = readFullWithEOFAsBoundary(s.logFile, payload)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Ignore a trailing partial frame (e.g., crash mid-write).
			return nil
		}
		if err != nil {
			return fmt.Errorf("read wal frame payload: %w", err)
		}

		var cmd walCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return fmt.Errorf("decode wal command: %w", err)
		}
		if !isValidOp(cmd.Op) {
			return fmt.Errorf("unknown wal op: %q", cmd.Op)
		}
		s.applyCommand(cmd)
	}
}

func (s *kvServer) appendWAL(cmd walCommand) error {
	if !isValidOp(cmd.Op) {
		return fmt.Errorf("unknown wal op: %q", cmd.Op)
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("encode wal command: %w", err)
	}
	if len(payload) > maxWALFrameSize {
		return fmt.Errorf("wal payload too large: %d bytes", len(payload))
	}
	frame := make([]byte, 4)
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	if err := writeFull(s.logFile, frame); err != nil {
		return fmt.Errorf("write wal frame length: %w", err)
	}
	if err := writeFull(s.logFile, payload); err != nil {
		return fmt.Errorf("write wal frame payload: %w", err)
	}
	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	return nil
}

func (s *kvServer) applyCommand(cmd walCommand) {
	switch cmd.Op {
	case "put", "swap":
		_ = s.tree.ReplaceOrInsert(item{key: cmd.Key, value: cmd.Value})
	case "delete":
		_ = s.tree.Delete(item{key: cmd.Key})
	}
}

func (s *kvServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.tree.Get(item{key: req.Key})
	if err := s.appendWAL(walCommand{Op: "put", Key: req.Key, Value: req.Value}); err != nil {
		return nil, err
	}
	s.applyCommand(walCommand{Op: "put", Key: req.Key, Value: req.Value})
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
	if err := s.appendWAL(walCommand{Op: "swap", Key: req.Key, Value: req.Value}); err != nil {
		return nil, err
	}
	s.applyCommand(walCommand{Op: "swap", Key: req.Key, Value: req.Value})

	if got == nil {
		return &kvpb.SwapReply{Found: false}, nil
	}
	return &kvpb.SwapReply{Found: true, OldValue: got.(item).value}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	del := s.tree.Get(item{key: req.Key})
	if err := s.appendWAL(walCommand{Op: "delete", Key: req.Key}); err != nil {
		return nil, err
	}
	s.applyCommand(walCommand{Op: "delete", Key: req.Key})
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
	logPath := flag.String("wal", "data/server.wal", "path to durable write-ahead log")
	flag.Parse()

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	srv, err := newKVServer(*logPath)
	if err != nil {
		log.Fatalf("server init failed: %v", err)
	}
	defer srv.logFile.Close()

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVSServer(grpcServer, srv)

	fmt.Printf("server listening on %s with wal %s\n", *listenAddr, *logPath)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
