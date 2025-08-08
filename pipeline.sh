#!/bin/bash

# Set up environment
cd /home/ubuntu/attraction-io
rm -rf input output temp work
mkdir -p input output temp work logs

# Pull latest code from GitHub
git reset --hard HEAD
git pull

# Run Julia pipeline (change path to your Julia bin if not in PATH)
julia --project=. scheduler/run_jobs.jl

# Auto-shutdown after run (stops the instance to save cost, not terminate)
sudo shutdown -h now
