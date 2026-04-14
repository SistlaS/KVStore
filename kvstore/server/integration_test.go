package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	kvpb "madkv/kvstore/gen/kvpb"
)

var (
	clientBinaryOnce sync.Once
	clientBinaryPath string
	clientBinaryErr  error
)

type testManager struct {
	addr       string
	listener   net.Listener
	grpcServer *grpc.Server
}

func (m *testManager) stop() {
	if m.grpcServer != nil {
		m.grpcServer.Stop()
	}
	if m.listener != nil {
		_ = m.listener.Close()
	}
}

type testManagerServer struct {
	kvpb.UnimplementedClusterManagerServer
	mu            sync.Mutex
	serverAddrs   []string
	serverRF      int
	numPartitions int
	registered    map[uint32]bool
}

func newTestManagerServer(serverAddrs []string, serverRF int) (*testManagerServer, error) {
	if serverRF <= 0 {
		return nil, fmt.Errorf("server replication factor must be positive")
	}
	if len(serverAddrs) == 0 || len(serverAddrs)%serverRF != 0 {
		return nil, fmt.Errorf("server address count %d must be a positive multiple of server_rf %d", len(serverAddrs), serverRF)
	}
	return &testManagerServer{
		serverAddrs:   append([]string(nil), serverAddrs...),
		serverRF:      serverRF,
		numPartitions: len(serverAddrs) / serverRF,
		registered:    make(map[uint32]bool, len(serverAddrs)),
	}, nil
}

func flattenTestServerID(partitionID, replicaID uint32, serverRF int) (uint32, error) {
	if serverRF <= 0 {
		return 0, fmt.Errorf("server replication factor must be positive")
	}
	return partitionID*uint32(serverRF) + replicaID, nil
}

func (m *testManagerServer) RegisterServer(ctx context.Context, req *kvpb.RegisterServerRequest) (*kvpb.RegisterServerReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if int(req.PartitionId) >= m.numPartitions {
		return nil, status.Errorf(codes.InvalidArgument, "partition id %d out of range", req.PartitionId)
	}
	if int(req.ReplicaId) >= m.serverRF {
		return nil, status.Errorf(codes.InvalidArgument, "replica id %d out of range", req.ReplicaId)
	}
	serverID, err := flattenTestServerID(req.PartitionId, req.ReplicaId, m.serverRF)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid server indices: %v", err)
	}
	if int(serverID) >= len(m.serverAddrs) {
		return nil, status.Errorf(codes.InvalidArgument, "server id %d out of range", serverID)
	}
	expected := m.serverAddrs[serverID]
	m.registered[serverID] = true
	return &kvpb.RegisterServerReply{
		NumPartitions:   uint32(m.numPartitions),
		ServerRf:        uint32(m.serverRF),
		AssignedApiAddr: expected,
	}, nil
}

func (m *testManagerServer) GetClusterInfo(ctx context.Context, req *kvpb.GetClusterInfoRequest) (*kvpb.GetClusterInfoReply, error) {
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

type testServer struct {
	srv         *kvServer
	apiListener net.Listener
	p2pListener net.Listener
	apiServer   *grpc.Server
	p2pServer   *grpc.Server
	cancel      context.CancelFunc
	stopOnce    sync.Once
}

func (s *testServer) stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.apiServer != nil {
			s.apiServer.Stop()
		}
		if s.p2pServer != nil {
			s.p2pServer.Stop()
		}
		if s.apiListener != nil {
			_ = s.apiListener.Close()
		}
		if s.p2pListener != nil {
			_ = s.p2pListener.Close()
		}
		if s.srv != nil && s.srv.db != nil {
			_ = s.srv.db.Close()
		}
	})
}

type integrationCluster struct {
	t              *testing.T
	numPartitions  int
	serverRF       int
	managerAddrs   []string
	managers       []*testManager
	servers        [][]*testServer
	serverAPIAddrs [][]string
	clientBinary   string
}

func reserveAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve addr: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close reserved addr: %v", err)
	}
	return addr
}

func buildClientBinary(t *testing.T) string {
	t.Helper()
	clientBinaryOnce.Do(func() {
		buildDir, err := os.MkdirTemp("", "madkv-client-bin-")
		if err != nil {
			clientBinaryErr = err
			return
		}
		clientBinaryPath = filepath.Join(buildDir, "client")
		cmd := exec.Command("go", "build", "-o", clientBinaryPath, "./client")
		cmd.Dir = filepath.Clean(filepath.Join(".."))
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			clientBinaryErr = fmt.Errorf("go build client failed: %w\n%s", err, out.String())
		}
	})
	if clientBinaryErr != nil {
		t.Fatalf("build client binary: %v", clientBinaryErr)
	}
	return clientBinaryPath
}

func startManager(t *testing.T, serverAddrs []string, serverRF int) *testManager {
	t.Helper()
	mgr, err := newTestManagerServer(serverAddrs, serverRF)
	if err != nil {
		t.Fatalf("newTestManagerServer: %v", err)
	}
	addr := reserveAddr(t)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen manager %s: %v", addr, err)
	}
	grpcServer := grpc.NewServer()
	kvpb.RegisterClusterManagerServer(grpcServer, mgr)
	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("manager serve %s: %v", addr, err)
		}
	}()
	return &testManager{addr: addr, listener: lis, grpcServer: grpcServer}
}

func startServer(t *testing.T, backerDir string, partitionID, replicaID, serverRF, numPartitions int, apiAddr, p2pAddr string, peerAddrs []string, managerAddrs []string) *testServer {
	t.Helper()
	srv, err := newKVServer(backerDir, partitionID, replicaID, serverRF, numPartitions, apiAddr, peerAddrs)
	if err != nil {
		t.Fatalf("newKVServer(partition=%d replica=%d): %v", partitionID, replicaID, err)
	}

	apiLis, err := net.Listen("tcp", apiAddr)
	if err != nil {
		_ = srv.db.Close()
		t.Fatalf("listen api %s: %v", apiAddr, err)
	}
	p2pLis, err := net.Listen("tcp", p2pAddr)
	if err != nil {
		_ = apiLis.Close()
		_ = srv.db.Close()
		t.Fatalf("listen p2p %s: %v", p2pAddr, err)
	}

	apiServer := grpc.NewServer()
	p2pServer := grpc.NewServer()
	kvpb.RegisterKVSServer(apiServer, srv)
	kvpb.RegisterRaftPeerServer(p2pServer, srv)

	runCtx, cancel := context.WithCancel(context.Background())
	go srv.electionLoop(runCtx)
	go srv.heartbeatLoop(runCtx)
	go func() {
		if err := p2pServer.Serve(p2pLis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("p2p serve partition=%d replica=%d: %v", partitionID, replicaID, err)
		}
	}()
	go func() {
		if err := apiServer.Serve(apiLis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("api serve partition=%d replica=%d: %v", partitionID, replicaID, err)
		}
	}()

	registerServerWithAllManagers(t, managerAddrs, partitionID, replicaID, apiAddr)

	return &testServer{
		srv:         srv,
		apiListener: apiLis,
		p2pListener: p2pLis,
		apiServer:   apiServer,
		p2pServer:   p2pServer,
		cancel:      cancel,
	}
}

func registerServerWithAllManagers(t *testing.T, managerAddrs []string, partitionID, replicaID int, apiAddr string) {
	t.Helper()
	var firstErr error
	for _, managerAddr := range managerAddrs {
		conn, err := grpc.NewClient(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			firstErr = err
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := kvpb.NewClusterManagerClient(conn).RegisterServer(ctx, &kvpb.RegisterServerRequest{
			PartitionId: uint32(partitionID),
			ReplicaId:   uint32(replicaID),
			ApiAddr:     apiAddr,
		})
		cancel()
		_ = conn.Close()
		if err != nil {
			firstErr = err
			continue
		}
		if resp == nil {
			firstErr = fmt.Errorf("nil register response for manager %s", managerAddr)
		}
	}
	if firstErr != nil {
		t.Logf("best-effort manager registration encountered an error: %v", firstErr)
	}
}

func newIntegrationCluster(t *testing.T, numPartitions, serverRF int) *integrationCluster {
	t.Helper()
	if numPartitions <= 0 {
		t.Fatalf("numPartitions must be positive")
	}
	if serverRF <= 0 {
		t.Fatalf("serverRF must be positive")
	}

	clientBin := buildClientBinary(t)

	serverAPIAddrs := make([][]string, numPartitions)
	serverP2PAddrs := make([][]string, numPartitions)
	flattenedAPIAddrs := make([]string, 0, numPartitions*serverRF)
	for part := 0; part < numPartitions; part++ {
		serverAPIAddrs[part] = make([]string, 0, serverRF)
		serverP2PAddrs[part] = make([]string, 0, serverRF)
		for replica := 0; replica < serverRF; replica++ {
			apiAddr := reserveAddr(t)
			p2pAddr := reserveAddr(t)
			serverAPIAddrs[part] = append(serverAPIAddrs[part], apiAddr)
			serverP2PAddrs[part] = append(serverP2PAddrs[part], p2pAddr)
			flattenedAPIAddrs = append(flattenedAPIAddrs, apiAddr)
		}
	}

	managers := make([]*testManager, 0, 3)
	managerAddrs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		mgr := startManager(t, flattenedAPIAddrs, serverRF)
		managers = append(managers, mgr)
		managerAddrs = append(managerAddrs, mgr.addr)
	}

	cluster := &integrationCluster{
		t:              t,
		numPartitions:  numPartitions,
		serverRF:       serverRF,
		managerAddrs:   managerAddrs,
		managers:       managers,
		servers:        make([][]*testServer, numPartitions),
		serverAPIAddrs: serverAPIAddrs,
		clientBinary:   clientBin,
	}

	backerRoot := t.TempDir()
	for part := 0; part < numPartitions; part++ {
		cluster.servers[part] = make([]*testServer, serverRF)
		for replica := 0; replica < serverRF; replica++ {
			peerAddrs := make([]string, 0, serverRF-1)
			for peer := 0; peer < serverRF; peer++ {
				if peer == replica {
					continue
				}
				peerAddrs = append(peerAddrs, serverP2PAddrs[part][peer])
			}
			cluster.servers[part][replica] = startServer(
				t,
				filepath.Join(backerRoot, fmt.Sprintf("p%d-r%d", part, replica)),
				part,
				replica,
				serverRF,
				numPartitions,
				serverAPIAddrs[part][replica],
				serverP2PAddrs[part][replica],
				peerAddrs,
				managerAddrs,
			)
		}
	}

	waitForClusterReady(t, managerAddrs, numPartitions, serverRF)
	waitForPartitionLeaders(t, cluster, 20*time.Second)

	t.Cleanup(cluster.stop)
	return cluster
}

func (c *integrationCluster) stop() {
	for part := range c.servers {
		for replica := range c.servers[part] {
			if c.servers[part][replica] != nil {
				c.servers[part][replica].stop()
			}
		}
	}
	for _, mgr := range c.managers {
		if mgr != nil {
			mgr.stop()
		}
	}
}

func waitForClusterReady(t *testing.T, managerAddrs []string, numPartitions, serverRF int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, managerAddr := range managerAddrs {
			conn, err := grpc.NewClient(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := kvpb.NewClusterManagerClient(conn).GetClusterInfo(ctx, &kvpb.GetClusterInfoRequest{})
			cancel()
			_ = conn.Close()
			if err == nil && resp.GetReady() && int(resp.GetNumPartitions()) == numPartitions && int(resp.GetServerRf()) == serverRF {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("cluster did not become ready within timeout")
}

func waitForPartitionLeaders(t *testing.T, cluster *integrationCluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for part := 0; part < cluster.numPartitions; part++ {
			if _, srv := cluster.partitionLeader(part); srv == nil {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leaders in all partitions")
}

func (c *integrationCluster) partitionLeader(partition int) (int, *testServer) {
	for replica, srv := range c.servers[partition] {
		if srv == nil {
			continue
		}
		srv.srv.mu.Lock()
		isLeader := srv.srv.role == roleLeader
		srv.srv.mu.Unlock()
		if isLeader {
			return replica, srv
		}
	}
	return -1, nil
}

func (c *integrationCluster) waitForPartitionLeader(partition int, timeout time.Duration) (int, *testServer) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if replica, srv := c.partitionLeader(partition); srv != nil {
			return replica, srv
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for a leader in partition %d", partition)
	return -1, nil
}

func (c *integrationCluster) killReplica(partition, replica int) {
	srv := c.servers[partition][replica]
	if srv == nil {
		c.t.Fatalf("missing server partition=%d replica=%d", partition, replica)
	}
	srv.stop()
	c.servers[partition][replica] = nil
}

func (c *integrationCluster) killFollower(partition int) int {
	leaderReplica, _ := c.waitForPartitionLeader(partition, 10*time.Second)
	for replica := 0; replica < c.serverRF; replica++ {
		if replica != leaderReplica && c.servers[partition][replica] != nil {
			c.killReplica(partition, replica)
			return replica
		}
	}
	c.t.Fatalf("no follower available to kill in partition %d", partition)
	return -1
}

func (c *integrationCluster) killLeader(partition int) int {
	leaderReplica, _ := c.waitForPartitionLeader(partition, 10*time.Second)
	if leaderReplica < 0 {
		c.t.Fatalf("no leader found in partition %d", partition)
	}
	c.killReplica(partition, leaderReplica)
	return leaderReplica
}

func (c *integrationCluster) killManager(idx int) {
	if idx < 0 || idx >= len(c.managers) {
		c.t.Fatalf("manager index %d out of range", idx)
	}
	if c.managers[idx] == nil {
		c.t.Fatalf("manager %d already stopped", idx)
	}
	c.managers[idx].stop()
	c.managers[idx] = nil
}

func (c *integrationCluster) keyForPartition(partition int) string {
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("key-%d-%d", partition, i)
		if ownerForKey(candidate, c.numPartitions) == partition {
			return candidate
		}
	}
	c.t.Fatalf("could not find key for partition %d", partition)
	return ""
}

func (c *integrationCluster) runClient(t *testing.T, timeout time.Duration, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fullArgs := append([]string{"--manager_addrs", strings.Join(c.managerAddrs, ",")}, args...)
	cmd := exec.CommandContext(ctx, c.clientBinary, fullArgs...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		t.Fatalf("client command failed: %v\noutput:\n%s", err, out.String())
	}
	return out.String()
}

func (c *integrationCluster) runClientExpectTimeout(t *testing.T, timeout time.Duration, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fullArgs := append([]string{"--manager_addrs", strings.Join(c.managerAddrs, ",")}, args...)
	cmd := exec.CommandContext(ctx, c.clientBinary, fullArgs...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err == nil {
		t.Fatalf("expected client request to time out, but it completed successfully\noutput:\n%s", out.String())
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("expected timeout, got error %v\noutput:\n%s", err, out.String())
	}
	return out.String()
}

func TestReplicatedClusterFailureScenarios(t *testing.T) {
	// Bring up the full 5-partition, RF-3 cluster and wait until every partition has a leader.
	cluster := newIntegrationCluster(t, 5, 3)

	// Pick one key from each partition so the failures below exercise different shards.
	partition0Key := cluster.keyForPartition(0)
	partition1Key := cluster.keyForPartition(1)
	partition2Key := cluster.keyForPartition(2)
	partition3Key := cluster.keyForPartition(3)
	partition4Key := cluster.keyForPartition(4)

	// Baseline check: the client can write and read successfully before any failures.
	out := cluster.runClient(t, 20*time.Second, "--op", "put", "--key", partition0Key, "--value", "one")
	if !strings.Contains(out, fmt.Sprintf("PUT %s one (found=false)", partition0Key)) {
		t.Fatalf("unexpected client output for initial put:\n%s", out)
	}
	out = cluster.runClient(t, 20*time.Second, "--op", "get", "--key", partition0Key)
	if !strings.Contains(out, fmt.Sprintf("GET %s one", partition0Key)) {
		t.Fatalf("unexpected client output for initial get:\n%s", out)
	}

	// Scenario 1: fail follower replicas in two different partitions.
	// The quorum should remain intact, so client requests still succeed.
	cluster.killFollower(0)
	cluster.killFollower(1)

	out = cluster.runClient(t, 20*time.Second, "--op", "put", "--key", partition0Key, "--value", "two")
	if !strings.Contains(out, fmt.Sprintf("PUT %s two (found=true)", partition0Key)) {
		t.Fatalf("unexpected client output after follower failure in partition 0:\n%s", out)
	}
	out = cluster.runClient(t, 20*time.Second, "--op", "put", "--key", partition1Key, "--value", "alpha")
	if !strings.Contains(out, fmt.Sprintf("PUT %s alpha (found=false)", partition1Key)) {
		t.Fatalf("unexpected client output after follower failure in partition 1:\n%s", out)
	}
	out = cluster.runClient(t, 20*time.Second, "--op", "get", "--key", partition0Key)
	if !strings.Contains(out, fmt.Sprintf("GET %s two", partition0Key)) {
		t.Fatalf("unexpected client output after follower failure in partition 0:\n%s", out)
	}
	out = cluster.runClient(t, 20*time.Second, "--op", "get", "--key", partition1Key)
	if !strings.Contains(out, fmt.Sprintf("GET %s alpha", partition1Key)) {
		t.Fatalf("unexpected client output after follower failure in partition 1:\n%s", out)
	}

	// Scenario 2: kill the current leader in one partition and wait for a new one.
	// Once election completes, client operations should continue normally.
	oldLeader := cluster.killLeader(2)
	newLeader, _ := cluster.waitForPartitionLeader(2, 20*time.Second)
	if newLeader == oldLeader {
		t.Fatalf("leader failure in partition 2 did not trigger a new leader")
	}
	out = cluster.runClient(t, 20*time.Second, "--op", "put", "--key", partition2Key, "--value", "bravo")
	if !strings.Contains(out, fmt.Sprintf("PUT %s bravo (found=false)", partition2Key)) {
		t.Fatalf("unexpected client output after leader failure in partition 2:\n%s", out)
	}
	out = cluster.runClient(t, 20*time.Second, "--op", "get", "--key", partition2Key)
	if !strings.Contains(out, fmt.Sprintf("GET %s bravo", partition2Key)) {
		t.Fatalf("unexpected client output after leader election in partition 2:\n%s", out)
	}

	// Scenario 3: stop one manager replica and show that clients can still join
	// and use the KV service through the remaining manager endpoints.
	cluster.killManager(0)
	out = cluster.runClient(t, 20*time.Second, "--op", "put", "--key", partition3Key, "--value", "charlie")
	if !strings.Contains(out, fmt.Sprintf("PUT %s charlie (found=false)", partition3Key)) {
		t.Fatalf("unexpected client output after manager failure:\n%s", out)
	}
	out = cluster.runClient(t, 20*time.Second, "--op", "get", "--key", partition3Key)
	if !strings.Contains(out, fmt.Sprintf("GET %s charlie", partition3Key)) {
		t.Fatalf("unexpected client output after manager failure on get:\n%s", out)
	}

	// Scenario 4: fail more than f replicas in a partition.
	// The partition should lose quorum, so the client request should time out
	// instead of returning an unsafe success.
	cluster.killReplica(4, 1)
	cluster.killReplica(4, 2)
	cluster.runClientExpectTimeout(t, 5*time.Second, "--op", "put", "--key", partition4Key, "--value", "delta")
}
