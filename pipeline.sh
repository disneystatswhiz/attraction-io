#!/usr/bin/env bash
set -euo pipefail

# ==============================
# Config (override via env vars)
# ==============================
: "${BRANCH:=ec2}"
: "${JULIA_BIN:=julia}"
: "${REPO_DIR:=$(pwd)}"
: "${LOG_DIR:=logs}"

# S3 destination (fixed file path — will overwrite each time)
: "${S3_URI:=s3://touringplans_stats/stats_work/ec2-logs/attraction-io-log.txt}"

# Upload toggle (set to 0 to skip)
: "${UPLOAD_TO_S3:=1}"

# -------------- helpers --------------
ts()  { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*"; }
need() { command -v "$1" >/dev/null 2>&1 || { log "WARN: '$1' not found"; return 1; }; }

# ============ go ============
cd "$REPO_DIR"

# 1) Update repo (best effort)
if need git; then
  log "Updating repo on branch ${BRANCH}"
  git fetch --all --prune || true
  git checkout "${BRANCH}" || true
  git pull --rebase --autostash origin "${BRANCH}" || true
else
  log "git not found; skipping repo update"
fi

# 2) Capture commit hash
GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
log "Commit: ${GIT_COMMIT}"

# 3) Ensure Julia deps
log "Instantiating Julia project"
"$JULIA_BIN" --project=. -e 'using Pkg; Pkg.instantiate()'

# 4) Run sequential loop (threads = auto)
export JULIA_NUM_THREADS=auto
mkdir -p "$LOG_DIR"
RUN_LOG="${LOG_DIR}/attraction-io-log.txt"

log "Starting sequential run (threads=$JULIA_NUM_THREADS)"
START_TS="$(date -Iseconds)"
set +e
"$JULIA_BIN" --project=. scheduler/run_all_parks.jl | tee "$RUN_LOG"
EXIT_CODE=${PIPESTATUS[0]}
set -e
END_TS="$(date -Iseconds)"

# 5) Optionally upload to S3 (overwrite same file)
if [[ "$UPLOAD_TO_S3" -eq 1 ]]; then
  if need aws; then
    log "Uploading log to S3 → ${S3_URI}"
    aws s3 cp --only-show-errors --no-progress "$RUN_LOG" "$S3_URI"
    log "Upload complete (overwrote previous log)."
  else
    log "AWS CLI not found; skipping S3 upload."
  fi
else
  log "UPLOAD_TO_S3=0; skipping S3 upload."
fi

# 6) Finish
if [[ $EXIT_CODE -eq 0 ]]; then
  log "✅ Run finished OK."
else
  log "❌ Run finished with errors (exit $EXIT_CODE)."
fi
exit $EXIT_CODE
