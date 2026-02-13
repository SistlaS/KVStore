# Always run commands from repo root
set shell := ["bash", "-cu"]


# --------
# Install dependencies and setup
# --------
deps:
    #!/usr/bin/env bash
    cd kvstore
    # Initialize go module if not exists
    if [ ! -f go.mod ]; then
        go mod init madkv/kvstore
    fi
    # Get dependencies
    go get google.golang.org/grpc
    go get google.golang.org/protobuf
    # Generate protobuf code
    mkdir -p gen/kvpb
    protoc --go_out=gen --go_opt=paths=source_relative \
           --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
           proto/test.proto
    # Tidy up dependencies
    go mod tidy
    echo "✓ Dependencies installed and protobuf code generated"

# --------
# Build
# --------
build:
    cd kvstore && mkdir -p bin
    cd kvstore && go build -o bin/server ./server
    cd kvstore && go build -o bin/client ./client
    @echo "✓ Built server and client binaries"

# --------
# Run server
# --------
server listen_addr="127.0.0.1:50051":
    cd kvstore && ./bin/server --listen {{listen_addr}}

# --------
# Run client (Ping for now)
# --------
client server_addr="127.0.0.1:50051" msg="hello":
    cd kvstore && ./bin/client --server {{server_addr}} --msg {{msg}}

# --------
# Clean build artifacts
# --------
clean:
    rm -rf kvstore/bin
    rm -rf kvstore/gen
    @echo "✓ Cleaned build artifacts"

# --------
# Kill server (best-effort)
# --------
kill:
    pkill -f "bin/server" || true
    @echo "✓ Killed server processes"
