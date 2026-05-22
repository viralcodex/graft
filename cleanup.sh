#!/bin/bash
set -euo pipefail

start_port="${1:-8000}"
num_instances="${2:-3}"

for ((i=0; i<num_instances; i++)); do
  port=$((start_port + i))
  pids="$(lsof -ti tcp:"$port" || true)"

  if [ -z "$pids" ]; then
    echo "No process listening on port $port"
    continue
  fi

  echo "Stopping process(es) on port $port: $pids"
  kill $pids
done