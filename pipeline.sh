#!/bin/bash
set -euo pipefail

# --- setup workspace ---
cd /home/ubuntu/attraction-io
rm -rf input output temp work
mkdir -p input output temp work logs

# --- get latest code ---
git reset --hard HEAD
git pull

# --- run your pipeline ---
julia --project=. scheduler/run_jobs.jl

# --- optional pause to flush logs ---
sleep 5

# --- stop the instance via OS shutdown ---
echo "Pipeline complete. Shutting down instance..."
sudo poweroff   # or: sudo shutdown -h now
