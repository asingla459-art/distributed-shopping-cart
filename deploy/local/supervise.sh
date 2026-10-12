#!/usr/bin/env bash
# Restart-on-crash loop for one instance (local stand-in for Docker's restart policy).
# usage: supervise.sh <n> <run_dir>
i=$1; RUN=$2
while true; do
  python3 -m uvicorn app.main:app --host 127.0.0.1 --port $((8000 + i)) --log-level warning --no-access-log &
  echo $! > "$RUN/app-$i.child"
  wait $!; rc=$?
  echo "$(date +%H:%M:%S.%3N) supervisor: app-$i exited (code $rc), restarting in ${RESTART_DELAY:-2}s" >> "$RUN/supervisor.log"
  sleep "${RESTART_DELAY:-2}"
done
