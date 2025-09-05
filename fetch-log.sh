#!/bin/bash
# save as fetch-log.sh

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
OUTPUT="pipeline_${TIMESTAMP}.log"

aws ssm send-command \
  --targets "Key=instanceIds,Values=i-0f24187fd8c69a39f" \
  --document-name "AWS-RunShellScript" \
  --profile stats \
  --region us-east-1 \
  --parameters 'commands=["sudo cat /home/ubuntu/attraction-io/logs/pipeline_latest.log"]' \
  --query "Command.CommandId" --output text > cmd_id.txt

CMD_ID=$(cat cmd_id.txt)

aws ssm list-command-invocations \
  --command-id "$CMD_ID" \
  --details \
  --region us-east-1 \
  --profile stats \
  --query "CommandInvocations[0].CommandPlugins[0].Output" \
  --output text > "$OUTPUT"

echo "✅ Log saved to $OUTPUT"
