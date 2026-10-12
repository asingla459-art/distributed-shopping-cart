#!/usr/bin/env bash
# Reproduce every experiment in the report (takes ~10 minutes).
#   experiments/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."
C=deploy/local/cluster.sh
fresh() { $C stop; rm -rf run; "$@"; }

# 1. correctness: naive baseline (with and without instance cache) vs consistent design
fresh env L1_CACHE_ENABLED=1 $C start naive 3;      python3 -m experiments.correctness naive
fresh env L1_CACHE_ENABLED=0 $C start naive 3;      python3 -m experiments.correctness naive-nocache
fresh env L1_CACHE_ENABLED=1 $C start consistent 3; python3 -m experiments.correctness consistent

# 2. failure experiment
fresh env L1_CACHE_ENABLED=0 $C start naive 3;      python3 -m experiments.failure naive-nocache
fresh env L1_CACHE_ENABLED=1 $C start consistent 3; python3 -m experiments.failure consistent

# 3. load tests: low / medium / high for three configurations
for cfg in "naive 1" "consistent 0" "consistent 1"; do
  set -- $cfg
  fresh env L1_CACHE_ENABLED=$2 $C start "$1" 3
  for lvl in "low 5" "medium 25" "high 75"; do
    set -- $cfg $lvl
    python3 -m loadtest.loadgen --users "$4" --duration 20 --label "$3" \
      --out "experiments/results/load_${1}_cache${2}_${3}.json" > /dev/null
    echo "load $1 cache=$2 $3 done"
  done
done
# 4. cache before/after with Redis one network hop away (+1 ms each way)
python3 experiments/latency_proxy.py 6390 6379 1 & LP=$!
for cfg in "netlat_cache0 0 round-robin" "netlat_cache1 1 round-robin" "netlat_cache1_affinity 1 user-hash"; do
  set -- $cfg
  fresh env REDIS_PORT=6390 L1_CACHE_ENABLED=$2 LB_POLICY=$3 $C start consistent 3
  for lvl in "low 5" "medium 25"; do
    set -- $cfg $lvl
    python3 -m loadtest.loadgen --users "$5" --duration 20 --label "$4" \
      --out "experiments/results/${1}_${4}.json" > /dev/null
  done
done
kill $LP
$C stop
python3 -m experiments.charts
