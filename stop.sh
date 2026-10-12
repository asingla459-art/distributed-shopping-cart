#!/usr/bin/env bash
# Stop every process started by ./start.sh
cd "$(dirname "$0")"
deploy/local/cluster.sh stop
echo "stopped"
