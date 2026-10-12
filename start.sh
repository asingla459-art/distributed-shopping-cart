#!/usr/bin/env bash
# ONE command to run the whole project (GitHub Codespaces, Ubuntu or WSL):
#   ./start.sh          -> installs what is missing, starts 3 instances + Redis + LB,
#                          runs the tests and the full demonstration
#   ./stop.sh           -> stops everything
set -e
cd "$(dirname "$0")"

echo "== 1/4 Checking tools =="
need=()
command -v redis-server >/dev/null || need+=(redis-server)
command -v jq >/dev/null || need+=(jq)
command -v curl >/dev/null || need+=(curl)
if [ ${#need[@]} -gt 0 ]; then
  echo "installing: ${need[*]}"
  sudo apt-get update -y -qq && sudo apt-get install -y -qq "${need[@]}"
fi
python3 -c "import starlette, uvicorn, jwt" 2>/dev/null || \
  python3 -m pip install -q -r requirements.txt 2>/dev/null || \
  python3 -m pip install -q --break-system-packages -r requirements.txt

echo "== 2/4 Starting the cluster (Redis + replica, 3 app instances, worker, load balancer) =="
deploy/local/cluster.sh stop >/dev/null 2>&1 || true
rm -rf run
deploy/local/cluster.sh start consistent 3

echo "== 3/4 Running the automated tests =="
python3 -m unittest -v tests.test_api

echo "== 4/4 Running the demonstration =="
experiments/demo.sh

echo
echo "All done. The API is running at http://localhost:8080  (try: curl http://localhost:8080/health)"
echo "Stop everything with: ./stop.sh"
