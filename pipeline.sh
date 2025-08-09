#!/bin/bash
set -euo pipefail

cd /home/ubuntu/attraction-io
rm -rf input output temp work
mkdir -p input output temp work logs

git reset --hard HEAD
git pull

stop_instance() {
    # IMDSv2 token
    TOKEN=$(curl -sS -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")

    # Instance ID + AZ from metadata
    IID=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id)

    AZ=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/placement/availability-zone)

    # Region = AZ without the trailing letter (e.g., "us-east-1c" -> "us-east-1")
    REG=${AZ::-1}

    echo "Stopping instance $IID in region $REG ..."
    aws ec2 stop-instances --instance-ids "$IID" --region "$REG"
}

trap 'sleep 5; stop_instance' EXIT

julia --project=. scheduler/run_jobs.jl
sleep 10   # let IO flush (optional)
