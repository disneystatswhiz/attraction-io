#!/bin/bash
set -euo pipefail

# ========= Config =========
PROJECT_DIR="/home/ubuntu/attraction-io"
KEEP_LOGS=14                 # how many pipeline_*.log files to keep
SHUTDOWN_ON_EXIT=0           # set to 0 to skip poweroff for debugging
PATH="/usr/local/bin:/usr/bin:/bin"   # cron-safe PATH
# ==========================

cd "$PROJECT_DIR"
mkdir -p input output temp work logs

# Fresh code
git reset --hard HEAD
git pull --ff-only

# Git info for the header
GIT_MSG="$(git log -1 --pretty=%s 2>/dev/null || echo 'n/a')"
GIT_DATE="$(git log -1 --date=iso-strict --pretty=%cd 2>/dev/null || echo 'n/a')"

# Timestamped log (UTC) + machine tag
TS="$(date -u +%F_%H%M%SZ)"
HOST="$(hostname -s || echo ec2)"
LOG_REL="logs/pipeline_${TS}_${HOST}.log"
LOG_ABS="${PROJECT_DIR}/${LOG_REL}"

# Mirror all stdout/stderr into the log (and still show on console if interactive)
exec > >(tee -a "$LOG_ABS") 2>&1

# Keep a "latest" symlink for easy tailing (no readlink needed)
ln -sfn "$LOG_ABS" "${PROJECT_DIR}/logs/pipeline_latest.log"

echo "==== PIPELINE BOOT CONTEXT ===="
date -Is
echo "whoami: $(whoami)"
echo "pwd:    $(pwd)"
echo "git date:  $GIT_DATE"
echo "git msg:    $GIT_MSG"
echo "julia:   $(command -v julia || echo 'not found')"
julia --version || true
timedatectl || true
echo "================================"

# >>> added: Julia environment bootstrap (cron-safe, self-healing)
export JULIA_PROJECT="$PROJECT_DIR"
export JULIA_DEPOT_PATH="/home/ubuntu/.julia"
export GKSwstype=nul                      # headless GR just in case
# optional: speedups/consistency
# export JULIA_NUM_THREADS=$(nproc)

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
'
# <<< added

# Rotate old logs (simple, deterministic)
echo "Rotating logs (keeping last $KEEP_LOGS)…"
ls -1t logs/pipeline_*.log 2>/dev/null | tail -n +$((KEEP_LOGS+1)) | xargs -r rm -f

# On EXIT: stamp status, flush, optionally shut down
finish() {
  RC=$?
  echo "==== PIPELINE EXIT $(date -Is) rc=$RC ===="
  sync || true; sleep 2
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
stdbuf -oL -eL julia --project="$JULIA_PROJECT" scheduler/run_jobs.jl