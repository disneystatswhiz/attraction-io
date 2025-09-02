#!/bin/bash
aws ssm start-session \
  --region us-east-1 \
  --profile stats \
  --target i-0f24187fd8c69a39f \
  --document-name AWS-StartInteractiveCommand \
  --parameters "command=[\"sudo -iu ubuntu bash -lc 'cd /home/ubuntu/attraction-io/logs && tail -n +1 -f pipeline_latest.log'\"]"
