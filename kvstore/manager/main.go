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
	mu            sync.Mutex
	serverAddrs   []string
	serverRF      int
	numPartitions int
	registered    map[uint32]bool
}

func newManagerServer(serverAddrs []string, serverRF int) (*managerServer, error) {
	if serverRF <= 0 {
		return nil, fmt.Errorf("server replication factor must be positive")
	}
	if len(serverAddrs) == 0 || len(serverAddrs)%serverRF != 0 {
		return nil, fmt.Errorf("server address count %d must be a positive multiple of server_rf %d", len(serverAddrs), serverRF)
	}
	return &managerServer{
		serverAddrs:   serverAddrs,
		serverRF:      serverRF,
		numPartitions: len(serverAddrs) / serverRF,
		registered:    make(map[uint32]bool, len(serverAddrs)),
	}, nil
}

func flattenServerID(partitionID, replicaID uint32, serverRF int) (uint32, error) {
	if serverRF <= 0 {
		return 0, fmt.Errorf("server replication factor must be positive")
	}
	return partitionID*uint32(serverRF) + replicaID, nil
}

func (m *managerServer) RegisterServer(ctx context.Context, req *kvpb.RegisterServerRequest) (*kvpb.RegisterServerReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if int(req.PartitionId) >= m.numPartitions {
		return nil, status.Errorf(codes.InvalidArgument, "partition id %d out of range", req.PartitionId)
	}
	if int(req.ReplicaId) >= m.serverRF {
		return nil, status.Errorf(codes.InvalidArgument, "replica id %d out of range", req.ReplicaId)
	}
	serverID, err := flattenServerID(req.PartitionId, req.ReplicaId, m.serverRF)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid server indices: %v", err)
	}
	if int(serverID) >= len(m.serverAddrs) {
		return nil, status.Errorf(codes.InvalidArgument, "server id %d out of range", serverID)
	}
	expected := m.serverAddrs[serverID]
	m.registered[serverID] = true
	return &kvpb.RegisterServerReply{
		NumPartitions: uint32(m.numPartitions),
		ServerRf:      uint32(m.serverRF),
		AssignedApiAddr: expected,
	}, nil
}

func (m *managerServer) GetClusterInfo(ctx context.Context, req *kvpb.GetClusterInfoRequest) (*kvpb.GetClusterInfoReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ready := len(m.registered) == len(m.serverAddrs)
	addrs := append([]string(nil), m.serverAddrs...)
	return &kvpb.GetClusterInfoReply{
		Ready:         ready,
		ServerAddrs:   addrs,
		ServerRf:      uint32(m.serverRF),
		NumPartitions: uint32(m.numPartitions),
	}, nil
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
	if len(out) > 500 {
		return nil, fmt.Errorf("too many servers (%d), max is 500", len(out))
	}
	return out, nil
}

func main() {
	replicaID := flag.Int("replica_id", 0, "manager replica ID")
	managerListen := flag.String("man_listen", "0.0.0.0:3666", "ip:port for manager client/server API")
	_ = flag.String("p2p_listen", "0.0.0.0:3606", "unused manager peer listener for non-replicated manager mode")
	_ = flag.String("peer_addrs", "none", "unused manager peers for non-replicated manager mode")
	serverRF := flag.Int("server_rf", 1, "server replication factor")
	servers := flag.String("server_addrs", "127.0.0.1:3777", "comma-separated list of server public addresses")
	_ = flag.String("backer_path", "./backer.m.0", "unused manager backer path for non-replicated manager mode")
	flag.Parse()

	serverAddrs, err := parseServers(*servers)
	if err != nil {
		log.Fatalf("invalid server_addrs arg: %v", err)
	}
	mgr, err := newManagerServer(serverAddrs, *serverRF)
	if err != nil {
		log.Fatalf("manager init failed: %v", err)
	}

	lis, err := net.Listen("tcp", *managerListen)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterClusterManagerServer(grpcServer, mgr)

	fmt.Printf("manager replica %d listening on %s with %d partitions rf=%d\n", *replicaID, *managerListen, mgr.numPartitions, mgr.serverRF)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
