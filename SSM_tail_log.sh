sudo -iu ubuntu bash -lc '
  LOGDIR="/home/ubuntu/attraction-io/logs"
  LATEST=$(ls -1t "$LOGDIR" 2>/dev/null | head -n 1)
  if [ -z "$LATEST" ]; then
    echo "No logs found in $LOGDIR"
    exit 1
  fi
  echo "Tailing: $LOGDIR/$LATEST"
  tail -n 50 -F "$LOGDIR/$LATEST"
'
