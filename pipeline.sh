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

# Which Julia job to run
JOB_SCRIPT="$PROJECT_DIR/scheduler/run_jobs.jl"     # adjust if yours lives elsewhere (e.g., src/run_jobs.jl)
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
        cache = joinpath(homedir(), ".julia", "compiled", "v$(VERSION.major).$(VERSION.minor)")
        try
            run(`rm -rf $cache`)
        catch
        end
        Pkg.instantiate(; verbose=true)
        Pkg.precompile()
    end
' || {
  echo "Julia bootstrap failed; aborting."
  exit 1
}

# ---------- Ensure Python + venv (for fact-table Step 0) ----------
ensure_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "Python3 present: $(python3 --version)"
    return
  fi
  echo "Python3 not found; installing…"
  if [ -f /etc/debian_version ]; then
    sudo apt-get update -y
    sudo apt-get install -y python3 python3-venv python3-pip
  elif [ -f /etc/redhat-release ]; then
    sudo yum install -y python3 || sudo dnf install -y python3
    python3 -m ensurepip --upgrade || sudo yum install -y python3-pip || true
  else
    echo "Unsupported distro — install Python3 manually."; exit 1
  fi
}

setup_venv() {
  VENV_DIR="$PROJECT_DIR/venv"
  if [ ! -d "$VENV_DIR" ]; then
    echo "Creating venv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi
  # shellcheck source=/dev/null
  source "$VENV_DIR/bin/activate"
  python -m pip install --upgrade pip
  # Minimal deps for your fact-table scripts; add more here if needed.
  python -m pip install boto3 pandas pyarrow s3fs
  export PYTHON_BIN="$VENV_DIR/bin/python"
}

echo "Bootstrapping Python environment…"
ensure_python
setup_venv

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

# ---------- Launch the job with live output + heartbeat ----------
echo "Starting Julia launcher at $(date -Is)…"
if [[ ! -f "$JOB_SCRIPT" ]]; then
  echo "Job script not found: $JOB_SCRIPT"
  exit 1
fi

# Force line-buffered output so logs flush immediately to console
if command -v script >/dev/null 2>&1; then
  script -q -f -c "julia --project=\"$JULIA_PROJECT\" \"$JOB_SCRIPT\"" /dev/null &
else
  stdbuf -oL -eL julia --project="$JULIA_PROJECT" "$JOB_SCRIPT" &
fi
JPID=$!

# --- Heartbeat: print a timestamp every minute so monitor sessions stay live ---
(
  while kill -0 "$JPID" 2>/dev/null; do
    echo "[hb] running..."
    sleep 60
  done
) &
HBPID=$!

# Wait for Julia to finish, then clean up the heartbeat
wait "$JPID"; RC=$?
kill "$HBPID" 2>/dev/null || true
wait "$HBPID" 2>/dev/null || true

echo "Julia process finished with exit code $RC"
exit "$RC"
