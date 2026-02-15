package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/google/btree"
	kvpb "madkv/kvstore/gen/kvpb"
	"google.golang.org/grpc"
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
}

func newKVServer() *kvServer {
	return &kvServer{tree: btree.New(8)}
}

func (s *kvServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.tree.ReplaceOrInsert(item{key: req.Key, value: req.Value})
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
	_ = s.tree.ReplaceOrInsert(item{key: req.Key, value: req.Value})
	
	if got == nil {
		return &kvpb.SwapReply{Found: false}, nil
	}
	return &kvpb.SwapReply{Found: true, OldValue: got.(item).value}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	del := s.tree.Delete(item{key: req.Key})
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
	flag.Parse()

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVSServer(grpcServer, newKVServer())

	fmt.Printf("server listening on %s\n", *listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
