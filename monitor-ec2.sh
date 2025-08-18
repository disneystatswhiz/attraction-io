#!/usr/bin/env bash
set -euo pipefail

HOST="ubuntu@54.145.104.219"
KEY='/d/Dropbox (TouringPlans.com)/stats team/pipeline/fred-ec2-key.pem'
REMOTE_DIR="/home/ubuntu/attraction-io"
SESSION="mon"

ssh -t -i "$KEY" -o StrictHostKeyChecking=no "$HOST" "bash -lc '
  set -euo pipefail

  # Make SESSION/REMOTE_DIR available in this shell
  SESSION=\"$SESSION\"
  REMOTE_DIR=\"$REMOTE_DIR\"

  # Ensure tmux exists
  if ! command -v tmux >/dev/null 2>&1; then
    sudo apt-get update -y && sudo apt-get install -y tmux
  fi

  cd \"$REMOTE_DIR\"

  # Start fresh session
  tmux has-session -t \"$SESSION\" 2>/dev/null && tmux kill-session -t \"$SESSION\"

  # Pane 1: full log then follow
  tmux new-session -d -s \"$SESSION\" \"tail -n +1 -F logs/pipeline_latest.log\"

  # Pane 2: live /work watcher
  tmux split-window -h -t \"$SESSION\":0 \"watch -n 2 ls -lh work/\"

  tmux select-layout -t \"$SESSION\" even-horizontal
  tmux set -g mouse on

  exec tmux attach -t \"$SESSION\"
'"
