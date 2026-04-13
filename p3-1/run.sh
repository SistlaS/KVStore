#!/bin/bash
set -exu

just p3 kill
rm -rf ./backer.*
rm -f ./s*.log
rm -f ./m*.log
rm -f ./r*.log
sleep 5
just p3 m0 &
just p3 kvs00 &
just p3 kvs01 &
just p3 kvs02 &
just p3 kvs10 &
just p3 kvs11 &
just p3 kvs12 
# just p3 c0


# rm -rf ./backer.*
# rm -f ./s*.log
# pkill -9 -x "raft" 2>/dev/null || true
# sleep 1
# just p3 rs0 &
# just p3 rs1 &
# just p3 rs2 &

# just p3 f0

# just p3 fuzz