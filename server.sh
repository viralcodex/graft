#!/bin/bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
binary_path="${2:-$script_dir/server}"

# Check if the number of instances is provided as an argument
if [ -z "${1:-}" ]; then
  echo "Usage: $0 <number_of_instances> [binary_path]"
  exit 1
fi

if [ ! -x "$binary_path" ]; then
  echo "Built binary not found or not executable: $binary_path"
  echo "Build it first with: go build -o $script_dir/server server.go"
  exit 1
fi

# Get the number of instances from the command line argument
num_instances=$1

# Start each instance on a different port starting from 8000
for ((i=0; i<num_instances; i++)); do
  port=$((8000 + i))
  node_id="node$((i + 1))"
  echo "Starting server instance $node_id on port $port"
  "$binary_path" -port "$port" -nodeId "$node_id" &
done

# Wait for all background processes to finish
wait