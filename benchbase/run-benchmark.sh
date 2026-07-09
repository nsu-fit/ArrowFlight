#!/usr/bin/env bash
set -euo pipefail

BENCHMARK="${1:-tpch}"
MODE="${2:-all}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
RESULTS="/benchbase/results"

usage() {
  cat >&2 <<'EOF'
Usage: bash benchbase/run-benchmark.sh <benchmark> <mode>

Benchmarks:
  tpch    TPC-H, supported here
  tpcds   TPC-DS, blocked until valid BenchBase config exists

Modes:
  all      create + load + execute
  create   create schema
  load     load data
  execute  run benchmark only
  clear    clear benchmark tables
  down     stop containers and remove PostgreSQL volume
  logs     follow PostgreSQL logs

Examples:
  bash benchbase/run-benchmark.sh tpch all
  bash benchbase/run-benchmark.sh tpch execute
  BENCH_POSTGRES_PORT=15432 bash benchbase/run-benchmark.sh tpch all
EOF
}

normalize_benchmark() {
  case "${BENCHMARK}" in
    tpc-h) BENCHMARK="tpch" ;;
    tpc-ds) BENCHMARK="tpcds" ;;
  esac
}

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

wait_postgres() {
  for _ in $(seq 1 60); do
    if compose exec -T postgres pg_isready -U admin -d benchbase >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done

  echo "PostgreSQL did not become ready in 120 seconds." >&2
  exit 1
}

start_postgres() {
  compose up -d postgres
  wait_postgres
}

bench_config() {
  echo "/benchbase/config/custom/postgres/${BENCHMARK}.xml"
}

run_benchbase() {
  start_postgres
  compose run --rm benchbase "$@"
}

normalize_benchmark

if [[ "${BENCHMARK}" == "tpcds" ]]; then
  cat >&2 <<'EOF'
TPC-DS is not enabled here.
BenchBase upstream currently ships an empty PostgreSQL sample_tpcds_config.xml.
Need valid TPC-DS schema/load/query config first, then this runner can use it.
EOF
  exit 2
fi

if [[ "${BENCHMARK}" != "tpch" ]]; then
  usage
  exit 2
fi

CONFIG="$(bench_config)"

case "${MODE}" in
  all)
    start_postgres
    compose run --rm benchbase -b "${BENCHMARK}" -c "${CONFIG}" --create=true --load=true --execute=true -d "${RESULTS}"
    ;;
  create)
    run_benchbase -b "${BENCHMARK}" -c "${CONFIG}" --create=true --load=false --execute=false -d "${RESULTS}"
    ;;
  load)
    run_benchbase -b "${BENCHMARK}" -c "${CONFIG}" --create=false --load=true --execute=false -d "${RESULTS}"
    ;;
  execute)
    run_benchbase -b "${BENCHMARK}" -c "${CONFIG}" --create=false --load=false --execute=true -d "${RESULTS}"
    ;;
  clear)
    run_benchbase -b "${BENCHMARK}" -c "${CONFIG}" --clear=true -d "${RESULTS}"
    ;;
  down)
    compose down -v
    ;;
  logs)
    compose logs -f postgres
    ;;
  help|--help|-h)
    usage
    ;;
  *)
    usage
    exit 2
    ;;
esac
