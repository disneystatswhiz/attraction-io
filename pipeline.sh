#!/bin/bash

# Set up environment
cd /home/ubuntu/attraction-io

# Pull latest code from GitHub
git pull

# Run Julia pipeline (change path to your Julia bin if not in PATH)
julia scheduler/run_jobs.jl

# Auto-shutdown after run (stops the instance to save cost, not terminate)
# sudo shutdown -h now
