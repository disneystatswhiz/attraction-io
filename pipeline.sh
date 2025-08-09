#!/bin/bash
set -euo pipefail

cd /home/ubuntu/attraction-io
rm -rf input output temp work
mkdir -p input output temp work logs

git reset --hard HEAD
git pull

stop_instance() {
  TOKEN=$(curl -sS -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
  IID=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id)
  REG=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/dynamic/instance-identity/document | awk -F\" '/region/ {print $4}')
  aws ec2 stop-instances --instance-ids "$IID" --region "$REG" || true
}

trap 'sleep 5; stop_instance' EXIT

julia --project=. scheduler/run_jobs.jl
sleep 10   # let IO flush (optional)
