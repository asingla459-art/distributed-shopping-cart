#!/usr/bin/env bash
# Run the whole system locally without Docker:
#   Redis primary + replica, N app instances, order worker, load balancer on :8080
#
#   deploy/local/cluster.sh start [consistent|naive] [instances]
#   deploy/local/cluster.sh stop
#   deploy/local/cluster.sh kill <n>        # hard-crash instance n (auto-restarted by its supervisor)
#   deploy/local/cluster.sh down <n>        # stop instance n permanently
#   deploy/local/cluster.sh restart <n>     # bring instance n back
set -euo pipefail
cd "$(dirname "$0")/../.."
RUN=${RUN_DIR:-run}
mkdir -p "$RUN"
MODE=${2:-consistent}
N=${3:-3}

# Each instance runs under a tiny supervisor loop that restarts it after a crash,
# like Docker's "restart: unless-stopped" policy in docker-compose.yml.
start_instance() {
  local i=$1 mode=$2
  INSTANCE_ID="app-$i" CONSISTENCY_MODE="$mode" LOG_FILE="$RUN/app-$i.jsonl" \
    nohup deploy/local/supervise.sh "$i" "$RUN" > "$RUN/app-$i.out" 2>&1 &
  echo $! > "$RUN/app-$i.pid"
}

case "${1:-}" in
start)
  redis-server --port 6379 --appendonly yes --appendfsync everysec --dir "$PWD/$RUN" \
    --daemonize yes --logfile "$PWD/$RUN/redis.log" --save ""
  mkdir -p "$RUN/replica"
  redis-server --port 6380 --replicaof 127.0.0.1 6379 --dir "$PWD/$RUN/replica" \
    --daemonize yes --logfile "$PWD/$RUN/redis-replica.log" --save ""
  for _ in $(seq 20); do redis-cli -p 6379 ping >/dev/null 2>&1 && break; sleep 0.2; done
  python3 -m app.seed
  echo "$MODE" > "$RUN/mode"
  ups=()
  for i in $(seq 1 "$N"); do start_instance "$i" "$MODE"; ups+=("127.0.0.1:$((8000 + i))"); done
  WORKER_ID=worker-1 nohup python3 -m app.worker > "$RUN/worker-1.out" 2>&1 & echo $! > "$RUN/worker-1.pid"
  nohup python3 deploy/local/lb.py 8080 "${ups[@]}" > "$RUN/lb.out" 2>&1 & echo $! > "$RUN/lb.pid"
  for i in $(seq 1 "$N"); do
    for _ in $(seq 50); do curl -sf "localhost:$((8000 + i))/health" >/dev/null && break; sleep 0.2; done
  done
  echo "cluster up: mode=$MODE instances=$N  ->  http://localhost:8080"
  ;;
stop)
  for f in "$RUN"/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
  for f in "$RUN"/*.child; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
  redis-cli -p 6380 shutdown nosave >/dev/null 2>&1 || true
  redis-cli -p 6379 shutdown nosave >/dev/null 2>&1 || true
  sleep 0.5
  ;;
kill)      # hard crash of the process; the supervisor restarts it (like Docker)
  kill -9 "$(cat "$RUN/app-$2.child")" && echo "killed app-$2 (supervisor will restart it)"
  ;;
down)      # stop instance n and its supervisor (stays down)
  kill "$(cat "$RUN/app-$2.pid")" 2>/dev/null || true; kill -9 "$(cat "$RUN/app-$2.child")" 2>/dev/null || true
  echo "app-$2 stopped"
  ;;
restart)
  start_instance "$2" "$(cat "$RUN/mode")"
  for _ in $(seq 50); do curl -sf "localhost:$((8000 + $2))/health" >/dev/null && break; sleep 0.2; done
  echo "app-$2 back"
  ;;
*) echo "usage: $0 start [mode] [n] | stop | kill <n> | down <n> | restart <n>"; exit 1;;
esac
