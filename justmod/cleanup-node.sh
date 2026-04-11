# Run this on any one of the 3 nodes:
#   bash cleanup-node.sh 0
#   bash cleanup-node.sh 1
#   bash cleanup-node.sh 2

cat > cleanup-node.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

node="${1:-}"
if [[ -z "$node" ]]; then
  echo "usage: $0 {0|1|2}"
  exit 1
fi

case "$node" in
  0)
    ports='3666|3606|3777|3707'
    ;;
  1)
    ports='3667|3607|3778|3708'
    ;;
  2)
    ports='3668|3608|3779|3709'
    ;;
  *)
    echo "invalid node: $node (expected 0, 1, or 2)"
    exit 1
    ;;
esac

just p3::kill || true
pkill -f 'target/release/service' || true
pkill -f 'bin/manager' || true
pkill -f 'bin/server' || true
pkill -f 'bin/client' || true
pkill -f 'runner' || true
rm -rf ./backer.*
ss -ltnp | egrep "$ports" || true
EOF

chmod +x cleanup-node.sh
