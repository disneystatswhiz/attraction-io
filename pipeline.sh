#!/bin/bash
# pipeline.sh — boot launcher for attraction-io
# Safe-by-default: supports a Git-controlled kill switch and low-disk guard.

set -euo pipefail

# ========= Config =========
PROJECT_DIR="/home/ubuntu/attraction-io"
KEEP_LOGS=14                         # how many pipeline_*.log files to keep
SHUTDOWN_ON_EXIT=1                   # set to 0 to keep instance running after job
MIN_FREE_GB=20                       # minimum free space required on /
PATH="/usr/local/bin:/usr/bin:/bin"  # cron-safe PATH
# ==========================

cd "$PROJECT_DIR"
mkdir -p input output temp work logs

# Always start from a clean tree and pull latest code (enables remote “brake”)
git reset --hard HEAD
git pull --ff-only

# ---------- Git info ----------
GIT_MSG="$(git log -1 --pretty=%s 2>/dev/null || echo 'n/a')"
GIT_DATE="$(git log -1 --date=iso-strict --pretty=%cd 2>/dev/null || echo 'n/a')"

# ---------- Log setup ----------
TS="$(date -u +%F_%H%M%SZ)"
HOST="$(hostname -s 2>/dev/null || echo ec2)"
LOG_REL="logs/pipeline_${TS}_${HOST}.log"
LOG_ABS="${PROJECT_DIR}/${LOG_REL}"

# Mirror stdout/stderr into a timestamped log file (and to console if interactive)
exec > >(tee -a "$LOG_ABS") 2>&1
ln -sfn "$LOG_ABS" "${PROJECT_DIR}/logs/pipeline_latest.log"

echo "==== PIPELINE BOOT CONTEXT ===="
date -Is
echo "whoami: $(whoami)"
echo "pwd:    $(pwd)"
echo "git date: $GIT_DATE"
echo "git msg:  $GIT_MSG"
echo "julia:   $(command -v julia || echo 'not found')"
julia --version || true
timedatectl || true
echo "================================"

# ---------- Environment for Julia ----------
export JULIA_PROJECT="$PROJECT_DIR"
export JULIA_DEPOT_PATH="/home/ubuntu/.julia"
export GKSwstype=nul  # headless plotting safety

# ---------- (A) Git-controlled kill switch ----------
DISABLE_FLAG="$PROJECT_DIR/.disable_on_boot"
if [[ -f "$DISABLE_FLAG" ]]; then
  echo "Disable flag present ($DISABLE_FLAG). Exiting without running."
  exit 0
fi

# ---------- (B) Low-disk guard with light cleanup ----------
FREE_GB=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
echo "Free space on / before cleanup: ${FREE_GB}G"
if (( FREE_GB < MIN_FREE_GB )); then
  echo "Only ${FREE_GB}G free; attempting targeted cleanup…"
  rm -rf "$PROJECT_DIR/work" "$PROJECT_DIR/temp" "$PROJECT_DIR/output" || true
  # prune older logs beyond KEEP_LOGS
  ls -1t logs/pipeline_*.log 2>/dev/null | tail -n +$((KEEP_LOGS+1)) | xargs -r rm -f
  # trim systemd journal to 2 days (safe)
  sudo journalctl --vacuum-time=2d || true

  FREE_GB=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
  echo "Free space on / after cleanup: ${FREE_GB}G"
  if (( FREE_GB < MIN_FREE_GB )); then
    echo "Still low disk (${FREE_GB}G < ${MIN_FREE_GB}G). Aborting to protect the instance."
    exit 1
  fi
fi

# ---------- Bootstrap Julia env (idempotent) ----------
echo "Bootstrapping Julia env (instantiate + precompile)…"
julia --project="$JULIA_PROJECT" -e '
    using Pkg
    try
        Pkg.Registry.update()
        Pkg.instantiate(; verbose=true)
        Pkg.precompile()
    catch e
        @warn "Pkg bootstrap failed, clearing compiled cache and retrying" exception=(e, catch_backtrace())
        cache = joinpath(homedir(), ".julia", "compiled", "v$(VERSION.major).$(VERSION.minor)")
        try
            run(`rm -rf $cache`)
        catch err
            @warn "Failed to remove compiled cache" exception=(err, catch_backtrace())
        end
        Pkg.instantiate(; verbose=true)
        Pkg.precompile()
    end
' || {
  echo "Julia bootstrap failed; aborting."
  exit 1
}

# ---------- Log rotation (deterministic) ----------
echo "Rotating logs (keeping last $KEEP_LOGS)…"
ls -1t logs/pipeline_*.log 2>/dev/null | tail -n +$((KEEP_LOGS+1)) | xargs -r rm -f

# ---------- Exit trap (optionally power off) ----------
finish() {
  RC=$?
  echo "==== PIPELINE EXIT $(date -Is) rc=$RC ===="
  sync || true; sleep 2
  if [[ "${SHUTDOWN_ON_EXIT}" -eq 1 ]]; then
    echo "Pipeline complete. Shutting down instance…"
    sudo poweroff || true
  else
    echo "Pipeline complete. Instance left running (SHUTDOWN_ON_EXIT=0)."
  fi
  exit $RC
}
trap finish EXIT

# ---------- Clean workspace for this run ----------
rm -rf input output temp work
mkdir -p input output temp work

# ---------- Launch the scheduler ----------
echo "Starting Julia launcher at $(date -Is)…"
stdbuf -oL -eL julia --project="$JULIA_PROJECT" scheduler/run_jobs.jl
