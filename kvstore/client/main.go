package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kvpb "madkv/kvstore/gen/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const requestIDMetadataKey = "x-request-id"

type routedClient struct {
	timeout       time.Duration
	retry         time.Duration
	partitions    [][]string
	leaderHintsMu sync.Mutex
	leaderHints   map[int]int
	connMu        sync.Mutex
	conns         map[string]*grpc.ClientConn
	clients       map[string]kvpb.KVSClient
	clientID      string
	nextReqID     uint64
}

func newRoutedClient(partitions [][]string, timeout, retry time.Duration) *routedClient {
	clientID := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	return &routedClient{
		timeout:     timeout,
		retry:       retry,
		partitions:  partitions,
		leaderHints: make(map[int]int, len(partitions)),
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]kvpb.KVSClient),
		clientID:    clientID,
	}
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

func (c *routedClient) close() {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	for addr, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, addr)
		delete(c.clients, addr)
	}
}

func (c *routedClient) ensureConn(addr string) (kvpb.KVSClient, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if cli := c.clients[addr]; cli != nil {
		return cli, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	c.clients[addr] = kvpb.NewKVSClient(conn)
	return c.clients[addr], nil
}

func (c *routedClient) resetConn(addr string) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if conn := c.conns[addr]; conn != nil {
		_ = conn.Close()
	}
	delete(c.conns, addr)
	delete(c.clients, addr)
}

func fetchClusterInfo(managerAddrs []string, timeout, retry time.Duration) [][]string {
	for {
		for _, managerAddr := range managerAddrs {
			conn, err := grpc.NewClient(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("manager dial failed (%s): %v", managerAddr, err)
				continue
			}
			mc := kvpb.NewClusterManagerClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			resp, err := mc.GetClusterInfo(ctx, &kvpb.GetClusterInfoRequest{})
			cancel()
			_ = conn.Close()
			if err != nil {
				log.Printf("manager query failed (%s): %v", managerAddr, err)
				continue
			}
			if !resp.Ready || len(resp.ServerAddrs) == 0 {
				log.Printf("manager %s not ready yet; retrying", managerAddr)
				continue
			}
			if resp.ServerRf == 0 || len(resp.ServerAddrs)%int(resp.ServerRf) != 0 {
				log.Printf("manager %s returned invalid topology", managerAddr)
				continue
			}
			partitions := make([][]string, 0, len(resp.ServerAddrs)/int(resp.ServerRf))
			for i := 0; i < len(resp.ServerAddrs); i += int(resp.ServerRf) {
				group := append([]string(nil), resp.ServerAddrs[i:i+int(resp.ServerRf)]...)
				partitions = append(partitions, group)
			}
			return partitions
		}
		time.Sleep(retry)
	}
}

func leaderHintFromError(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return "", false
	}
	if st.Code() != codes.FailedPrecondition {
		return "", false
	}
	const prefix = "not leader:"
	msg := st.Message()
	if !strings.HasPrefix(msg, prefix) {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(msg, prefix)), true
}

func (c *routedClient) getReplicaOrder(partition int) []int {
	c.leaderHintsMu.Lock()
	defer c.leaderHintsMu.Unlock()
	n := len(c.partitions[partition])
	out := make([]int, 0, n)
	if hint, ok := c.leaderHints[partition]; ok && hint >= 0 && hint < n {
		out = append(out, hint)
	}
	for i := 0; i < n; i++ {
		if len(out) > 0 && out[0] == i {
			continue
		}
		out = append(out, i)
	}
	return out
}

func (c *routedClient) setLeaderHint(partition int, addr string) {
	if addr == "" {
		return
	}
	c.leaderHintsMu.Lock()
	defer c.leaderHintsMu.Unlock()
	for idx, candidate := range c.partitions[partition] {
		if candidate == addr {
			c.leaderHints[partition] = idx
			return
		}
	}
}

func (c *routedClient) nextMutationRequestID() string {
	seq := atomic.AddUint64(&c.nextReqID, 1)
	return c.clientID + "-" + strconv.FormatUint(seq, 10)
}

func (c *routedClient) callPartition(partition int, fn func(context.Context, kvpb.KVSClient) error) {
	for {
		order := c.getReplicaOrder(partition)
		for _, idx := range order {
			addr := c.partitions[partition][idx]
			client, err := c.ensureConn(addr)
			if err != nil {
				log.Printf("server dial failed (%s): %v", addr, err)
				c.resetConn(addr)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			err = fn(ctx, client)
			cancel()
			if err == nil {
				c.leaderHintsMu.Lock()
				c.leaderHints[partition] = idx
				c.leaderHintsMu.Unlock()
				return
			}
			if leaderAddr, ok := leaderHintFromError(err); ok {
				c.setLeaderHint(partition, leaderAddr)
			}
			log.Printf("server rpc failed (%s): %v", addr, err)
			c.resetConn(addr)
		}
		time.Sleep(c.retry)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage (CLI mode):
  client --manager_addrs <a,b,c> --op put    --key <k> --value <v>
  client --manager_addrs <a,b,c> --op get    --key <k>
  client --manager_addrs <a,b,c> --op swap   --key <k> --value <v>
  client --manager_addrs <a,b,c> --op delete --key <k>
  client --manager_addrs <a,b,c> --op scan   --start <k1> --end <k2>

Usage (stdin/stdout mode):
  client --manager_addrs <a,b,c>
`)
}

func main() {
	managerAddrsRaw := flag.String("manager_addrs", "127.0.0.1:3666", "comma-separated manager ip:port list")
	op := flag.String("op", "", "operation: put|get|swap|delete|scan")
	key := flag.String("key", "", "key for put/get/swap/delete")
	value := flag.String("value", "", "value for put/swap")
	start := flag.String("start", "", "scan start key")
	end := flag.String("end", "", "scan end key")
	timeout := flag.Duration("timeout", 2*time.Second, "rpc timeout")
	retry := flag.Duration("retry_interval", time.Second, "retry interval")
	flag.Usage = usage
	flag.Parse()

	managerAddrs := parseCommaList(*managerAddrsRaw)
	if len(managerAddrs) == 0 {
		log.Fatalf("manager_addrs must not be empty")
	}
	partitions := fetchClusterInfo(managerAddrs, *timeout, *retry)
	rc := newRoutedClient(partitions, *timeout, *retry)
	defer rc.close()

	if *op != "" {
		cliMode(rc, strings.ToLower(*op), *key, *value, *start, *end)
	} else {
		stdinMode(rc)
	}
}

func cliMode(c *routedClient, op, key, value, start, end string) {
	switch op {
	case "put":
		if key == "" || value == "" {
			log.Fatalf("put requires --key and --value")
		}
		var resp *kvpb.PutReply
		reqID := c.nextMutationRequestID()
		partition := ownerForKey(key, len(c.partitions))
		c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			ctx = metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, reqID)
			resp, err = cli.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
			return err
		})
		fmt.Printf("PUT %s %s (found=%v)\n", key, value, resp.Found)
	case "get":
		if key == "" {
			log.Fatalf("get requires --key")
		}
		var resp *kvpb.GetReply
		partition := ownerForKey(key, len(c.partitions))
		c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			resp, err = cli.Get(ctx, &kvpb.GetRequest{Key: key})
			return err
		})
		if !resp.Found {
			fmt.Printf("GET %s null\n", key)
		} else {
			fmt.Printf("GET %s %s\n", key, resp.Value)
		}
	case "swap":
		if key == "" || value == "" {
			log.Fatalf("swap requires --key and --value")
		}
		var resp *kvpb.SwapReply
		reqID := c.nextMutationRequestID()
		partition := ownerForKey(key, len(c.partitions))
		c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			ctx = metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, reqID)
			resp, err = cli.Swap(ctx, &kvpb.SwapRequest{Key: key, Value: value})
			return err
		})
		if !resp.Found {
			fmt.Printf("SWAP %s null\n", key)
		} else {
			fmt.Printf("SWAP %s old=%s new=%s\n", key, resp.OldValue, value)
		}
	case "delete":
		if key == "" {
			log.Fatalf("delete requires --key")
		}
		var resp *kvpb.DeleteReply
		reqID := c.nextMutationRequestID()
		partition := ownerForKey(key, len(c.partitions))
		c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			ctx = metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, reqID)
			resp, err = cli.Delete(ctx, &kvpb.DeleteRequest{Key: key})
			return err
		})
		fmt.Printf("DELETE %s (found=%v)\n", key, resp.Found)
	case "scan":
		if start == "" || end == "" {
			log.Fatalf("scan requires --start and --end")
		}
		pairs := scanAll(c, start, end)
		fmt.Printf("SCAN %s %s (%d pairs)\n", start, end, len(pairs))
		for _, p := range pairs {
			fmt.Printf("  %s %s\n", p.Key, p.Value)
		}
	default:
		log.Fatalf("unknown --op %q (expected put|get|swap|delete|scan)", op)
	}
}

func scanAll(c *routedClient, startKey, endKey string) []*kvpb.KVPair {
	if startKey == endKey {
		partition := ownerForKey(startKey, len(c.partitions))
		var resp *kvpb.ScanReply
		c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			resp, err = cli.Scan(ctx, &kvpb.ScanRequest{StartKey: startKey, EndKey: endKey})
			return err
		})
		return resp.Pairs
	}

	merged := make([]*kvpb.KVPair, 0)
	for partition := range c.partitions {
		var resp *kvpb.ScanReply
		c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			resp, err = cli.Scan(ctx, &kvpb.ScanRequest{StartKey: startKey, EndKey: endKey})
			return err
		})
		merged = append(merged, resp.Pairs...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })
	return merged
}

func stdinMode(c *routedClient) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				log.Printf("PUT requires 2 arguments: key value")
				continue
			}
			k, v := parts[1], parts[2]
			partition := ownerForKey(k, len(c.partitions))
			var resp *kvpb.PutReply
			reqID := c.nextMutationRequestID()
			c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
				var err error
				ctx = metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, reqID)
				resp, err = cli.Put(ctx, &kvpb.PutRequest{Key: k, Value: v})
				return err
			})
			if resp.Found {
				fmt.Printf("PUT %s found\n", k)
			} else {
				fmt.Printf("PUT %s not_found\n", k)
			}
		case "GET":
			if len(parts) < 2 {
				log.Printf("GET requires 1 argument: key")
				continue
			}
			k := parts[1]
			partition := ownerForKey(k, len(c.partitions))
			var resp *kvpb.GetReply
			c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
				var err error
				resp, err = cli.Get(ctx, &kvpb.GetRequest{Key: k})
				return err
			})
			if !resp.Found {
				fmt.Printf("GET %s null\n", k)
			} else {
				fmt.Printf("GET %s %s\n", k, resp.Value)
			}
		case "SWAP":
			if len(parts) < 3 {
				log.Printf("SWAP requires 2 arguments: key value")
				continue
			}
			k, v := parts[1], parts[2]
			partition := ownerForKey(k, len(c.partitions))
			var resp *kvpb.SwapReply
			reqID := c.nextMutationRequestID()
			c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
				var err error
				ctx = metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, reqID)
				resp, err = cli.Swap(ctx, &kvpb.SwapRequest{Key: k, Value: v})
				return err
			})
			if !resp.Found {
				fmt.Printf("SWAP %s null\n", k)
			} else {
				fmt.Printf("SWAP %s %s\n", k, resp.OldValue)
			}
		case "DELETE":
			if len(parts) < 2 {
				log.Printf("DELETE requires 1 argument: key")
				continue
			}
			k := parts[1]
			partition := ownerForKey(k, len(c.partitions))
			var resp *kvpb.DeleteReply
			reqID := c.nextMutationRequestID()
			c.callPartition(partition, func(ctx context.Context, cli kvpb.KVSClient) error {
				var err error
				ctx = metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, reqID)
				resp, err = cli.Delete(ctx, &kvpb.DeleteRequest{Key: k})
				return err
			})
			if resp.Found {
				fmt.Printf("DELETE %s found\n", k)
			} else {
				fmt.Printf("DELETE %s not_found\n", k)
			}
		case "SCAN":
			if len(parts) < 3 {
				log.Printf("SCAN requires 2 arguments: start_key end_key")
				continue
			}
			startKey, endKey := parts[1], parts[2]
			pairs := scanAll(c, startKey, endKey)
			fmt.Printf("SCAN %s %s BEGIN\n", startKey, endKey)
			for _, pair := range pairs {
				fmt.Printf("  %s %s\n", pair.Key, pair.Value)
			}
			fmt.Println("SCAN END")
		case "STOP":
			if len(parts) != 1 {
				log.Printf("STOP takes no arguments")
				continue
			}
			fmt.Println("STOP")
			return
		default:
			log.Printf("unknown command: %s", cmd)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("scanner error: %v", err)
	}
}
