package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kvpb "madkv/kvstore/gen/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func usage() {
	fmt.Fprintf(os.Stderr, `Usage (CLI mode):
  client --server <ip:port> --op put    --key <k> --value <v>
  client --server <ip:port> --op get    --key <k>
  client --server <ip:port> --op swap   --key <k> --value <v>
  client --server <ip:port> --op delete --key <k>
  client --server <ip:port> --op scan   --start <k1> --end <k2>

Usage (stdin/stdout mode):
  client --server <ip:port>
  (then read operations from stdin, one per line)

Examples:
  client --server 127.0.0.1:50051 --op put --key a --value 1
  client --server 127.0.0.1:50051 --op get --key a
  client --server 127.0.0.1:50051 --op scan --start a --end z
`)
}

func main() {
	serverAddr := flag.String("server", "127.0.0.1:50051", "server ip:port")
	op := flag.String("op", "", "operation: put|get|swap|delete|scan")
	key := flag.String("key", "", "key for put/get/swap/delete")
	value := flag.String("value", "", "value for put/swap")
	start := flag.String("start", "", "scan start key")
	end := flag.String("end", "", "scan end key")
	timeout := flag.Duration("timeout", 2*time.Second, "rpc timeout")
	flag.Usage = usage
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	c := kvpb.NewKVSClient(conn)

	// If --op is provided, run in CLI mode; otherwise, run in stdin/stdout mode
	if *op != "" {
		cliMode(c, *timeout, *op, *key, *value, *start, *end)
	} else {
		stdinMode(c, *timeout)
	}
}

func cliMode(c kvpb.KVSClient, timeout time.Duration, op, key, value, start, end string) {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch strings.ToLower(op) {
	case "put":
		if key == "" || value == "" {
			log.Fatalf("put requires --key and --value")
		}
		resp, err := c.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		if err != nil {
			log.Fatalf("Put RPC failed: %v", err)
		}
		fmt.Printf("PUT %s %s (found=%v)\n", key, value, resp.Found)

	case "get":
		if key == "" {
			log.Fatalf("get requires --key")
		}
		resp, err := c.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			log.Fatalf("Get RPC failed: %v", err)
		}
		if !resp.Found {
			fmt.Printf("GET %s null\n", key)
		} else {
			fmt.Printf("GET %s %s\n", key, resp.Value)
		}

	case "swap":
		if key == "" || value == "" {
			log.Fatalf("swap requires --key and --value")
		}
		resp, err := c.Swap(ctx, &kvpb.SwapRequest{Key: key, Value: value})
		if err != nil {
			log.Fatalf("Swap RPC failed: %v", err)
		}
		if !resp.Found {
			fmt.Printf("SWAP %s null\n", key)
		} else {
			fmt.Printf("SWAP %s old=%s new=%s\n", key, resp.OldValue, value)
		}

	case "delete":
		if key == "" {
			log.Fatalf("delete requires --key")
		}
		resp, err := c.Delete(ctx, &kvpb.DeleteRequest{Key: key})
		if err != nil {
			log.Fatalf("Delete RPC failed: %v", err)
		}
		fmt.Printf("DELETE %s (found=%v)\n", key, resp.Found)

	case "scan":
		if start == "" || end == "" {
			log.Fatalf("scan requires --start and --end")
		}
		resp, err := c.Scan(ctx, &kvpb.ScanRequest{StartKey: start, EndKey: end})
		if err != nil {
			log.Fatalf("Scan RPC failed: %v", err)
		}
		fmt.Printf("SCAN %s %s (%d pairs)\n", start, end, len(resp.Pairs))
		for _, p := range resp.Pairs {
			fmt.Printf("  %s %s\n", p.Key, p.Value)
		}

	default:
		log.Fatalf("unknown --op %q (expected put|get|swap|delete|scan)", op)
	}
}

func stdinMode(c kvpb.KVSClient, timeout time.Duration) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // Allow large lines for scan results

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		// Execute the command and print response
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				log.Printf("PUT requires 2 arguments: key value")
				cancel()
				continue
			}
			k, v := parts[1], parts[2]
			resp, err := c.Put(ctx, &kvpb.PutRequest{Key: k, Value: v})
			if err != nil {
				log.Printf("Put RPC failed: %v", err)
				cancel()
				continue
			}
			found := "not_found"
			if resp.Found {
				found = "found"
			}
			fmt.Printf("PUT %s %s\n", k, found)

		case "GET":
			if len(parts) < 2 {
				log.Printf("GET requires 1 argument: key")
				cancel()
				continue
			}
			k := parts[1]
			resp, err := c.Get(ctx, &kvpb.GetRequest{Key: k})
			if err != nil {
				log.Printf("Get RPC failed: %v", err)
				cancel()
				continue
			}
			if !resp.Found {
				fmt.Printf("GET %s null\n", k)
			} else {
				fmt.Printf("GET %s %s\n", k, resp.Value)
			}

		case "SWAP":
			if len(parts) < 3 {
				log.Printf("SWAP requires 2 arguments: key value")
				cancel()
				continue
			}
			k, v := parts[1], parts[2]
			resp, err := c.Swap(ctx, &kvpb.SwapRequest{Key: k, Value: v})
			if err != nil {
				log.Printf("Swap RPC failed: %v", err)
				cancel()
				continue
			}
			if !resp.Found {
				fmt.Printf("SWAP %s null\n", k)
			} else {
				fmt.Printf("SWAP %s %s\n", k, resp.OldValue)
			}

		case "DELETE":
			if len(parts) < 2 {
				log.Printf("DELETE requires 1 argument: key")
				cancel()
				continue
			}
			k := parts[1]
			resp, err := c.Delete(ctx, &kvpb.DeleteRequest{Key: k})
			if err != nil {
				log.Printf("Delete RPC failed: %v", err)
				cancel()
				continue
			}
			found := "not_found"
			if resp.Found {
				found = "found"
			}
			fmt.Printf("DELETE %s %s\n", k, found)

		case "SCAN":
			if len(parts) < 3 {
				log.Printf("SCAN requires 2 arguments: start_key end_key")
				cancel()
				continue
			}
			startKey, endKey := parts[1], parts[2]
			resp, err := c.Scan(ctx, &kvpb.ScanRequest{StartKey: startKey, EndKey: endKey})
			if err != nil {
				log.Printf("Scan RPC failed: %v", err)
				cancel()
				continue
			}
			fmt.Printf("SCAN %s %s BEGIN\n", startKey, endKey)
			for _, pair := range resp.Pairs {
				fmt.Printf("  %s %s\n", pair.Key, pair.Value)
			}
			fmt.Println("SCAN END")

		case "STOP":
			if len(parts) != 1 {
				log.Printf("STOP takes no arguments")
				cancel()
				continue
			}
			cancel()
			return

		default:
			log.Printf("unknown command: %s", cmd)
		}

		cancel()
	}

	// Handle scanner errors
	if err := scanner.Err(); err != nil {
		log.Fatalf("scanner error: %v", err)
	}
}
