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
	"strings"
	"time"

	kvpb "madkv/kvstore/gen/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type routedClient struct {
	serverAddrs []string
	timeout     time.Duration
	retry       time.Duration
	conns       []*grpc.ClientConn
	clients     []kvpb.KVSClient
}

func newRoutedClient(serverAddrs []string, timeout, retry time.Duration) *routedClient {
	return &routedClient{
		serverAddrs: serverAddrs,
		timeout:     timeout,
		retry:       retry,
		conns:       make([]*grpc.ClientConn, len(serverAddrs)),
		clients:     make([]kvpb.KVSClient, len(serverAddrs)),
	}
}

func ownerForKey(key string, numServers int) int {
	if numServers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(numServers))
}

func (c *routedClient) close() {
	for i := range c.conns {
		if c.conns[i] != nil {
			_ = c.conns[i].Close()
			c.conns[i] = nil
			c.clients[i] = nil
		}
	}
}

func (c *routedClient) ensureConn(serverID int) (kvpb.KVSClient, error) {
	if c.clients[serverID] != nil {
		return c.clients[serverID], nil
	}
	conn, err := grpc.NewClient(c.serverAddrs[serverID], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[serverID] = conn
	c.clients[serverID] = kvpb.NewKVSClient(conn)
	return c.clients[serverID], nil
}

func (c *routedClient) resetConn(serverID int) {
	if c.conns[serverID] != nil {
		_ = c.conns[serverID].Close()
	}
	c.conns[serverID] = nil
	c.clients[serverID] = nil
}

func fetchClusterInfo(managerAddr string, timeout, retry time.Duration) []string {
	for {
		conn, err := grpc.NewClient(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("manager dial failed: %v", err)
			time.Sleep(retry)
			continue
		}
		mc := kvpb.NewClusterManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := mc.GetClusterInfo(ctx, &kvpb.GetClusterInfoRequest{})
		cancel()
		_ = conn.Close()
		if err != nil {
			log.Printf("manager query failed: %v", err)
			time.Sleep(retry)
			continue
		}
		if !resp.Ready || len(resp.ServerAddrs) == 0 {
			log.Printf("manager not ready yet; retrying")
			time.Sleep(retry)
			continue
		}
		return resp.ServerAddrs
	}
}

func (c *routedClient) callSingleServer(serverID int, fn func(context.Context, kvpb.KVSClient) error) {
	for {
		client, err := c.ensureConn(serverID)
		if err != nil {
			log.Printf("server dial failed (%s): %v", c.serverAddrs[serverID], err)
			time.Sleep(c.retry)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		err = fn(ctx, client)
		cancel()
		if err == nil {
			return
		}

		log.Printf("server rpc failed (%s): %v; retrying", c.serverAddrs[serverID], err)
		c.resetConn(serverID)
		time.Sleep(c.retry)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage (CLI mode):
  client --manager <ip:port> --op put    --key <k> --value <v>
  client --manager <ip:port> --op get    --key <k>
  client --manager <ip:port> --op swap   --key <k> --value <v>
  client --manager <ip:port> --op delete --key <k>
  client --manager <ip:port> --op scan   --start <k1> --end <k2>

Usage (stdin/stdout mode):
  client --manager <ip:port>
`)
}

func main() {
	managerAddr := flag.String("manager", "127.0.0.1:3666", "manager ip:port")
	op := flag.String("op", "", "operation: put|get|swap|delete|scan")
	key := flag.String("key", "", "key for put/get/swap/delete")
	value := flag.String("value", "", "value for put/swap")
	start := flag.String("start", "", "scan start key")
	end := flag.String("end", "", "scan end key")
	timeout := flag.Duration("timeout", 2*time.Second, "rpc timeout")
	retry := flag.Duration("retry_interval", time.Second, "retry interval")
	flag.Usage = usage
	flag.Parse()

	serverAddrs := fetchClusterInfo(*managerAddr, *timeout, *retry)
	rc := newRoutedClient(serverAddrs, *timeout, *retry)
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
		sid := ownerForKey(key, len(c.serverAddrs))
		c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			resp, err = cli.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
			return err
		})
		fmt.Printf("PUT %s %s (found=%v)\n", key, value, resp.Found)

	case "get":
		if key == "" {
			log.Fatalf("get requires --key")
		}
		var resp *kvpb.GetReply
		sid := ownerForKey(key, len(c.serverAddrs))
		c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
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
		sid := ownerForKey(key, len(c.serverAddrs))
		c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
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
		sid := ownerForKey(key, len(c.serverAddrs))
		c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
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
		sid := ownerForKey(startKey, len(c.serverAddrs))
		var resp *kvpb.ScanReply
		c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
			var err error
			resp, err = cli.Scan(ctx, &kvpb.ScanRequest{StartKey: startKey, EndKey: endKey})
			return err
		})
		return resp.Pairs
	}

	merged := make([]*kvpb.KVPair, 0)
	for sid := range c.serverAddrs {
		var resp *kvpb.ScanReply
		c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
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
			sid := ownerForKey(k, len(c.serverAddrs))
			var resp *kvpb.PutReply
			c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
				var err error
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
			sid := ownerForKey(k, len(c.serverAddrs))
			var resp *kvpb.GetReply
			c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
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
			sid := ownerForKey(k, len(c.serverAddrs))
			var resp *kvpb.SwapReply
			c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
				var err error
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
			sid := ownerForKey(k, len(c.serverAddrs))
			var resp *kvpb.DeleteReply
			c.callSingleServer(sid, func(ctx context.Context, cli kvpb.KVSClient) error {
				var err error
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
