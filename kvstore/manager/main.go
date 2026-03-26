package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	kvpb "madkv/kvstore/gen/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type managerServer struct {
	kvpb.UnimplementedClusterManagerServer
	mu          sync.Mutex
	serverAddrs []string
	registered  map[uint32]bool
}

func newManagerServer(serverAddrs []string) *managerServer {
	return &managerServer{
		serverAddrs: serverAddrs,
		registered:  make(map[uint32]bool, len(serverAddrs)),
	}
}

func (m *managerServer) RegisterServer(ctx context.Context, req *kvpb.RegisterServerRequest) (*kvpb.RegisterServerReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if int(req.ServerId) >= len(m.serverAddrs) {
		return nil, status.Errorf(codes.InvalidArgument, "server id %d out of range", req.ServerId)
	}
	m.registered[req.ServerId] = true
	return &kvpb.RegisterServerReply{
		NumServers:      uint32(len(m.serverAddrs)),
		AssignedApiAddr: m.serverAddrs[req.ServerId],
	}, nil
}

func (m *managerServer) GetClusterInfo(ctx context.Context, req *kvpb.GetClusterInfoRequest) (*kvpb.GetClusterInfoReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ready := len(m.registered) == len(m.serverAddrs)
	addrs := append([]string(nil), m.serverAddrs...)
	return &kvpb.GetClusterInfoReply{Ready: ready, ServerAddrs: addrs}, nil
}

func parseServers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("servers list is empty")
	}
	if len(out) > 50 {
		return nil, fmt.Errorf("too many servers (%d), max is 50", len(out))
	}
	return out, nil
}

func main() {
	listenAddr := flag.String("listen", "0.0.0.0:3666", "ip:port for manager")
	servers := flag.String("servers", "127.0.0.1:3777", "comma-separated list of server public addresses")
	flag.Parse()

	serverAddrs, err := parseServers(*servers)
	if err != nil {
		log.Fatalf("invalid servers arg: %v", err)
	}

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterClusterManagerServer(grpcServer, newManagerServer(serverAddrs))

	fmt.Printf("manager listening on %s with %d servers\n", *listenAddr, len(serverAddrs))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
