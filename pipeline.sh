#!/usr/bin/env bash
set -e

# --- basic settings ---
REPO_URL="https://github.com/disneystatswhiz/attraction-io.git"   # or https://github.com/your-org/attraction-io.git
BRANCH="fact_table_sync"                                           # <--- change this to whatever branch you want
REPO_DIR="$HOME/attraction-io"
LOG_FILE="/tmp/ec2-log.txt"
S3_LOG="s3://touringplans_stats/stats_work/ec2-logs/fact_table_log.txt"
# -----------------------

echo "=== Starting pipeline run for branch: $BRANCH ==="

# ---------------------------------------------- #
# Steps:
# ---------------------------------------------- #

# ---------------------------------------------- #
# 1) pull the attraction-io pipeline from git
if [ -d "$REPO_DIR/.git" ]; then
  echo "Updating existing repo..."
  git -C "$REPO_DIR" fetch --quiet origin "$BRANCH"
  git -C "$REPO_DIR" checkout --quiet "$BRANCH"
  git -C "$REPO_DIR" reset --hard "origin/$BRANCH" --quiet
else
  echo "Cloning fresh repo..."
  git clone --branch "$BRANCH" --depth 1 "$REPO_URL" "$REPO_DIR"
fi

# ---------------------------------------------- #
# 2) run attraction-io/src/fact_table/main.py
cd "$REPO_DIR"
echo "Running main.py..."
python3 src/fact_table/main.py | tee "$LOG_FILE"

# ---------------------------------------------- #
# 3) upload log to S3 (overwrite)
echo "Uploading log to S3..."
aws s3 cp "$LOG_FILE" "$S3_LOG" --quiet

# ---------------------------------------------- #
# 4) shutdown instance
echo "Shutting down instance..."
sudo shutdown -h now
