#!/bin/bash
set -euo pipefail

# ========= Config =========
PROJECT_DIR="/home/ubuntu/attraction-io"
KEEP_LOGS=14                 # how many pipeline_*.log files to keep
SHUTDOWN_ON_EXIT=1           # set to 0 to skip poweroff for debugging
# ==========================

cd "$PROJECT_DIR"
mkdir -p input output temp work logs

# Fresh code
git reset --hard HEAD
git pull --ff-only

# Timestamped log (UTC) + machine tag
TS="$(date -u +%F_%H%M%SZ)"
HOST="$(hostname -s || echo ec2)"
LOG="logs/pipeline_${TS}_${HOST}.log"

# Mirror all stdout/stderr into the log (and still show on console if interactive)
exec > >(tee -a "$LOG") 2>&1

# Keep a "latest" symlink for easy tailing
ln -sfn "$LOG" logs/pipeline_latest.log

echo "==== PIPELINE BOOT CONTEXT ===="
date -Is
echo "whoami: $(whoami)"
echo "pwd:    $(pwd)"
echo "git sha: $(git rev-parse --short HEAD 2>/dev/null || echo 'n/a')"
echo "julia:   $(command -v julia || echo 'not found')"
julia --version || true
timedatectl || true
echo "================================"

# Rotate old logs (simple, deterministic)
echo "Rotating logs (keeping last $KEEP_LOGS)…"
ls -1t logs/pipeline_*.log 2>/dev/null | tail -n +$((KEEP_LOGS+1)) | xargs -r rm -f

# On EXIT: stamp status, flush, optionally shut down
finish() {
  RC=$?
  echo "==== PIPELINE EXIT $(date -Is) rc=$RC ===="
  # small flush
  sync || true
  sleep 2
  if [[ "${SHUTDOWN_ON_EXIT}" -eq 1 ]]; then
    echo "Pipeline complete. Shutting down instance..."
    sudo poweroff || true
  else
    echo "Pipeline complete. Instance left running (SHUTDOWN_ON_EXIT=0)."
  fi
  exit $RC
}
trap finish EXIT

# --- clean workspace for this run ---
rm -rf input output temp work
mkdir -p input output temp work

# -------- run the launcher (logs captured) --------
echo "Starting Julia launcher at $(date -Is)…"
# If you renamed the launcher file, update below:
julia --project=. scheduler/run_jobs.jl

# (No code after this point runs if Julia exits non-zero; finish() will still execute)
