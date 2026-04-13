#!/bin/bash

export PATH="$HOME/.cargo/bin:$PATH"

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <server-ip:port>"
    exit 1
fi

SERVER_IP=$1

echo "Connecting to KVStore at $SERVER_IP and deleting all keys..."
echo "DELETE_ALL" | cargo run --bin kvclient $SERVER_IP