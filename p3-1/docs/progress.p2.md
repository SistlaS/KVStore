# Project 2

## Agendas

We assume a fixed, known number of servers when launching the KV store service. Also, assume the number of servers is no larger than 50 (so decisions could be based on even a single ASCII character); in our tests and benchmarks, we will use a much smaller number than that.
1. Storage backend: 
    * A durable log which is crash consistent (no snapshot for now), and its counterpart in-memory kv store for fast performance.
    * Serialization/deserialization between in-memory language-specific data structures and their compact bytes representation on durable storage using protobuf
2. Server recovery: Upon restart, servers will load persisted state and replay log to recover previous states before serving requests. 
3. Keyspace Partitioning: partition the keyspace into disjoint subsets, distribute them to the  servers, and let each server take care of its assigned subset(s).  On crash, those key partions will become unavailable.
4. Cluster manager: Clients and servers query the manager to determine partition mappings.
    TODO: verify server id unique
5. Error Handling:  Whenever a KV store component A is establishing connection to another component B but fails due to B's unresponsiveness, A should implement an indefinite retry logic spaced by some wait time. Cases include:
    * server -> manager when manager is not up yet
    * client -> manager when manager is not up yet or when not all servers have been registered yet
    * client -> a server when that server is unresponsive
6. Consistency of Scans: We relax the consistency requirement on Scans. the Scan operation as a whole does not have to appear "atomic".


Example execution:
```bash
./yourmanager --man_listen 0.0.0.0:3666 --servers 1.2.3.4:3777,5.6.7.8:3778,9.10.11.12:3779

./yourserver --manager_addr 1.2.3.4:3666 --api_listen 0.0.0.0:3778 --server_id 1 --backer_path ./backer.1

./yourclient --manager_addr 1.2.3.4:3666
```