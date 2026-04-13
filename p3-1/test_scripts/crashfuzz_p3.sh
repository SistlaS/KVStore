#!/bin/bash

SESSION=kvcluster_fuzz
MANAGER_PORT=3666
P2P_PORT=3667
KV_START_PORT=3700
P2P_START_PORT=3800
N_PARTS=1
SERVER_RF=5

read -p "Do you want to crash two replicas during fuzzing? [y/N] " answer

if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
  echo "You must crash ≤ f = 2 replicas (including the leader)"

  read -p "Enter first replica ID to crash (should be the leader): " CRASH1
  read -p "Enter second replica ID to crash (follower): " CRASH2

  for rep_id in $CRASH1 $CRASH2; do
    WIN="s0${rep_id}"
    echo " Killing replica $rep_id (tmux window: $WIN)..."
    tmux send-keys -t $SESSION:$WIN C-c
    sleep 0.5
    tmux kill-window -t $SESSION:$WIN
  done

  echo ""
  echo "Crashes simulated. Continue fuzzing — system should still make progress!"
else
  echo " No servers crashed. System remains fully available."
fi

# ----------------------
# Done (leave system running)
# ----------------------

echo ""
echo "System running. Fuzzer can continue operating."
echo "You can shut down manually using:"
echo "  just p3::kill"
