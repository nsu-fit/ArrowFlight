#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

echo "=== Building BenchBase Docker image with TPC-DS plugin ==="
cd "${REPO_ROOT}"
docker build -t benchbase-tpcds:latest \
  -f "${SCRIPT_DIR}/Dockerfile" .

echo ""
echo "=== Done ==="
echo "Image: benchbase-tpcds:latest"
echo ""
echo "Run benchmarks with:"
echo "  BENCHBASE_IMAGE=benchbase-tpcds:latest bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpcds smoke"
