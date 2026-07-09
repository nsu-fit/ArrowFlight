#!/usr/bin/env bash
set -euo pipefail

BENCHMARK="${1:-tpch}"
MODE="${2:-all}"
QUERY_SET="${3:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
RESULTS="/benchbase/results"
GENERATED_CONFIG_LOCAL=""

usage() {
  cat >&2 <<'EOF'
Usage: bash benchbase/run-benchmark.sh <benchmark> <mode> [queries]

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

Queries:
  Optional for TPC-H execute/all. Examples: q6, q1,q6,q14, 1,6,14

Examples:
  bash benchbase/run-benchmark.sh tpch all
  bash benchbase/run-benchmark.sh tpch execute
  bash benchbase/run-benchmark.sh tpch execute q6
  bash benchbase/run-benchmark.sh tpch execute q1,q6,q14
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

cleanup_generated_config() {
  if [[ -n "${GENERATED_CONFIG_LOCAL}" && -f "${GENERATED_CONFIG_LOCAL}" ]]; then
    rm -f "${GENERATED_CONFIG_LOCAL}"
  fi
}

query_weights() {
  local normalized="${QUERY_SET,,}"
  normalized="${normalized// /}"

  local weights=()
  for _ in $(seq 1 22); do
    weights+=(0)
  done

  IFS=',' read -ra tokens <<< "${normalized}"
  for token in "${tokens[@]}"; do
    token="${token#q}"
    if [[ -z "${token}" || ! "${token}" =~ ^[0-9]+$ ]]; then
      echo "Bad TPC-H query selector: ${QUERY_SET}" >&2
      echo "Use q6 or q1,q6,q14." >&2
      exit 2
    fi

    local query_id=$((10#${token}))
    if (( query_id < 1 || query_id > 22 )); then
      echo "TPC-H query must be between q1 and q22: q${query_id}" >&2
      exit 2
    fi

    weights[$((query_id - 1))]=1
  done

  local joined="${weights[0]}"
  for i in $(seq 1 21); do
    joined+=",${weights[$i]}"
  done
  echo "${joined}"
}

prepare_config() {
  if [[ -z "${QUERY_SET}" ]]; then
    CONFIG="$(bench_config)"
    return
  fi

  if [[ "${MODE}" != "all" && "${MODE}" != "execute" ]]; then
    echo "Query subset is supported only for all/execute modes." >&2
    exit 2
  fi

  local base_config="${SCRIPT_DIR}/config/postgres/${BENCHMARK}.xml"
  local safe_selector="${QUERY_SET,,}"
  safe_selector="${safe_selector//[^a-z0-9,]/-}"
  GENERATED_CONFIG_LOCAL="${SCRIPT_DIR}/config/postgres/.${BENCHMARK}-${safe_selector}.xml"

  local weights
  weights="$(query_weights)"
  sed "s#<weights>.*</weights>#      <weights>${weights}</weights>#" "${base_config}" > "${GENERATED_CONFIG_LOCAL}"
  CONFIG="/benchbase/config/custom/postgres/$(basename "${GENERATED_CONFIG_LOCAL}")"
  trap cleanup_generated_config EXIT
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

prepare_config

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
