# Key-Value Store with gRPC

A simple Key-Value Store implemented using Rust and gRPC. 

## Modules

- `kvserver/`: The gRPC-based key-value store server.
- `kvclient/`: A simple gRPC client that interacts with the server.
- `kvstore/`: The core key-value store logic.
    - Put(key, new_value) -> key found or not
    - Swap(key, new_value) -> old_value if found, else null
    - Get(key) -> value if found, else null
    - Scan(start_key, end_key) -> ordered list of key-value pairs in range, inclusive
    - Delete(key) -> key found or not
- `kvrpc/`: The gRPC protocol definition using Protocol Buffers.

### Structure
```sh
│── kvserver/       
│   ├── src/main.rs     # Server logic using Tonic
│   ├── Cargo.toml  
│── kvclient/       
│   ├── src/main.rs     # Client implementation
│   ├── Cargo.toml  
│── kvstore/        
│   ├── src/kvstore.rs  # Implements the actual store functionality
│   ├── Cargo.toml  
│── kvrpc/          
│   ├── kvrpc.proto     # Defines RPC methods and message formats
│── kvtester/          
│   ├── src/main.rs     # Test Suite
│   ├── src/test_case1.rs
│   ├── src/test_case2.rs
│   ├── src/test_case3.rs
│   ├── src/test_case4.rs
│   ├── src/test_case5.rs
│   ├── Cargo.toml 
```


## Build & Run

### Install dependencies

First, install dependencies as mentioned in the README.md of starter code.
```sh
# Install Protobuf Compiler (Linux)
sudo apt-get update
sudo apt-get install -y protobuf-compiler
```

### Build project
```sh
cargo build
```
or 
```
just p1 build
```

### Run server
Start the key-value store server:

```sh
cargo run --bin kvserver -- 0.0.0.0:5739
```
or 
```sh
just p1 server
```
or 
```sh
just p1 server 0.0.0.0:5739
```


You should see:
```
kvserver running on [::1]:5739
```

### Run client
Open a new terminal and start the client:

```sh
cargo run --bin kvclient -- 127.0.0.1:5739
```
or
```sh
just p1 client
```
or 
```sh
just p1 client 0.0.0.0:5739
```

### Run YCSB workload

Run server:
```sh
just p1 server
```

Run benchmark 'a' with 5 clients:
```sh
just p1 bench 5 a
```

### Run KVTester or Test Suite

Run server:
```sh
just p1 server
```
or 
```sh
cargo run --bin kvserver -- 0.0.0.0:5739
```

Run test suite: 
```sh
just test1 server="127.0.0.1:3777"
```
or 
```sh
cargo run --bin kvtester 1 -- 0.0.0.0:5739
```
You can choose any number between 1-5.

## Todo
- Add multi-threaded support using `Arc<Mutex<KVStore>>`
- Optimize read-heavy workloads with `RwLock`
- Implement 5 test cases
- Implement fuzzy testing
- Add all just commands 
- Run YCSB workload and generate results