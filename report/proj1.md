# CS 739 MadKV Project 1

**Group members**: Name `email`, Name `email`

## Design Walkthrough

Code structure: 

The project is split into three parts:
-  `kvstore/server/main.go` for the gRPC server, 
- `kvstore/client/main.go` for the CLI + stdin/stdout client, and 
- `.proto` files under `kvstore/proto/` with generated stubs in `kvstore/gen/kvpb/`. 

The Justfile recipes in `justmod/proj1.just` drive builds and tests, while the core logic lives in the server and client binaries. This keeps API definitions, server state, and test automation clearly separated.

Server design: 

The server holds all data in an in-memory `btree.BTree` guarded by a single `sync.Mutex`, so each RPC is serialized for correctness. `PUT`, `GET`, and `DELETE` are direct tree operations, and `SCAN` uses an ascending iterator to return an inclusive range. `SWAP` returns the old value if present and inserts/updates the stored value atomically under the lock (like an UPSERT).

RPC protocol: 

The protocol is a simple unary gRPC API defined in `kvstore.proto` with `Put/Get/Swap/Delete/Scan` RPCs and explicit request/reply messages. The client supports a structured stdin/out format (one command per line) and prints normalized responses (`found`/`not_found`, `null`, `SCAN BEGIN/END`) that the provided tests and harnesses can parse. In CLI mode it also exposes flags for one-off RPCs to simplify manual debugging.

## Self-provided Testcases

<u>Found the following testcase results:</u> 1, 2, 3, 4, 5

You will run some testcases during demo time.

### Explanations

*FIXME: add your explanations of each testcase*

## Fuzz Testing

<u>Parsed the following fuzz testing results:</u>

num_clis | conflict | outcome
:-: | :-: | :-:
1 | no | PASSED
3 | no | PASSED
3 | yes | PASSED

You will run a multi-client conflicting-keys fuzz test during demo time.

### Comments

*FIXME: add your comments on fuzz testing*

## YCSB Benchmarking

<u>Single-client throughput/latency across workloads:</u>

![single-cli](plots-p1/ycsb-single-cli.png)

<u>Agg. throughput trend vs. number of clients:</u>

![tput-trend](plots-p1/ycsb-tput-trend.png)

<u>Avg. latency trend vs. number of clients:</u>

![lats-trend](plots-p1/ycsb-lats-trend.png)

### Comments

*FIXME: add your discussions of benchmarking results*

## Additional Discussion

*OPTIONAL: add extra discussions if applicable*
