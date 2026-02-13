curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh


sudo apt update
sudo apt install tree just default-jre liblog4j2-java protobuf-compiler golang-go 

#go codeplugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go get github.com/google/btree

export PATH="$PATH:$(go env GOPATH)/bin"

cd madkv
go mod init madkv
go mod tidy

cd kvstore
mkdir -p gen/kvpb
protoc -I proto   --go_out=gen/kvpb --go_opt=paths=source_relative   --go-grpc_out=gen/kvpb --go-grpc_opt=paths=source_relative   proto/kvstore.proto 


go build -o bin/server ./server
go build -o bin/client ./client

./bin/server --listen 127.0.0.1:50051
./bin/client --server 127.0.0.1:50051
