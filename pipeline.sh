#!/bin/bash
set -euo pipefail

# Force instance-role creds (ignore any leftover local profiles/keys)
/usr/bin/env -i bash <<'EOF'

unset AWS_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_DEFAULT_REGION

cd /home/ubuntu/attraction-io
rm -rf input output temp work
mkdir -p input output temp work logs

git reset --hard HEAD
git pull

# --- run your pipeline ---
julia --project=. scheduler/run_jobs.jl
sleep 10

# --- deterministic self-stop using IMDSv2 ---
TOKEN=$(curl -sS -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
REG=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/placement/region)
IID=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/instance-id)

echo "Stopping instance $IID in $REG ..."
aws --region "$REG" ec2 stop-instances --instance-ids "$IID"
EOF
