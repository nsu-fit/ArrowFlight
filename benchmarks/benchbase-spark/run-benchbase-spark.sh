#!/usr/bin/env bash
set -euo pipefail

BENCHMARK="${1:-tpch}"
MODE="${2:-smoke}"
QUERY_SET="${3:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
CONFIG_DIR="${SCRIPT_DIR}/config"
RESULTS_IN_CONTAINER="/benchbase/results"
GENERATED_CONFIG_LOCAL=""
EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/${BENCHMARK}.xml"
FLIGHT_SERVER_SERVICES=()

usage() {
  cat >&2 <<'EOF'
Usage: bash benchmarks/benchbase-spark/run-benchbase-spark.sh <benchmark> <mode> [queries]

Benchmarks:
  tpch
  tpcds

Modes:
  smoke    reset volumes, DuckDB generate Parquet, register Flight, execute small query set
  fresh    reset volumes, DuckDB generate Parquet, register Flight, execute benchmark
  prepare  reset volumes, DuckDB generate Parquet, register Flight, start Spark Thrift
  run      execute BenchBase against already prepared Spark Flight tables
  report   build latest HTML report
  down     stop stack and remove Docker volumes
  logs     follow compose logs

Queries:
  TPC-H only: q6, q1,q6,q14, 1,6,14
EOF
}

compose() {
  docker compose -f "${REPO_ROOT}/docker-compose.yml" "$@"
}

normalize_benchmark() {
  case "${BENCHMARK}" in
    tpc-h) BENCHMARK="tpch" ;;
    tpc-ds) BENCHMARK="tpcds" ;;
  esac
}

join_by_comma() {
  local IFS=,
  echo "$*"
}

read_cluster_nodes() {
  local props="${REPO_ROOT}/src/main/resources/arrowflight.properties"
  local nodes
  nodes="$(awk -F= '/^[[:space:]]*clusterNodes[[:space:]]*=/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' "${props}")"
  nodes="${nodes:-3}"

  if [[ ! "${nodes}" =~ ^[0-9]+$ ]] || (( nodes < 1 || nodes > 3 )); then
    echo "Bad clusterNodes=${nodes}. Supported: 1..3." >&2
    exit 2
  fi

  echo "${nodes}"
}

configure_cluster() {
  local nodes
  nodes="$(read_cluster_nodes)"

  local hosts=()
  local servers=()
  local data_dirs=()
  FLIGHT_SERVER_SERVICES=()

  for index in $(seq 1 "${nodes}"); do
    hosts+=("flight-server-${index}")
    servers+=("flight-server-${index}:32010")
    data_dirs+=("/server-data/flight-server-${index}")
    FLIGHT_SERVER_SERVICES+=("flight-server-${index}")
  done

  export FLIGHT_HOSTS
  export FLIGHT_SERVERS
  export FLIGHT_SERVER_DATA_DIRS
  export FLIGHT_SOURCE_HOST="flight-server-1"
  export FLIGHT_SOURCE_PORT="32010"
  export BENCHMARK_DATASET="${BENCHMARK}"
  export BENCHMARK_SCHEMA="${BENCHMARK}"
  export BENCHMARK_SCALE_FACTOR="${BENCHMARK_SCALE_FACTOR:-0.01}"

  FLIGHT_HOSTS="$(join_by_comma "${hosts[@]}")"
  FLIGHT_SERVERS="$(join_by_comma "${servers[@]}")"
  FLIGHT_SERVER_DATA_DIRS="$(join_by_comma "${data_dirs[@]}")"

  echo "clusterNodes=${nodes}"
  echo "FLIGHT_HOSTS=${FLIGHT_HOSTS}"
}

prepare_results_dir() {
  mkdir -p "${RESULTS_DIR}"
  chmod a+rwx "${RESULTS_DIR}" 2>/dev/null || true
}

cleanup_generated_config() {
  if [[ -n "${GENERATED_CONFIG_LOCAL}" && -f "${GENERATED_CONFIG_LOCAL}" ]]; then
    rm -f "${GENERATED_CONFIG_LOCAL}"
  fi
}

tpch_query_weights() {
  local normalized="${QUERY_SET,,}"
  normalized="${normalized// /}"

  local weights=()
  local selected=()
  for _ in $(seq 1 22); do
    weights+=(0)
  done

  IFS=',' read -ra tokens <<< "${normalized}"
  for token in "${tokens[@]}"; do
    token="${token#q}"
    if [[ -z "${token}" || ! "${token}" =~ ^[0-9]+$ ]]; then
      echo "Bad TPC-H query selector: ${QUERY_SET}" >&2
      exit 2
    fi

    local query_id=$((10#${token}))
    if (( query_id < 1 || query_id > 22 )); then
      echo "TPC-H query must be between q1 and q22: q${query_id}" >&2
      exit 2
    fi

    selected+=("${query_id}")
  done

  local count="${#selected[@]}"
  local base=$((100 / count))
  local rest=$((100 - base * count))
  local index=0
  for query_id in "${selected[@]}"; do
    local value="${base}"
    if (( index < rest )); then
      value=$((value + 1))
    fi
    weights[$((query_id - 1))]="${value}"
    index=$((index + 1))
  done

  join_by_comma "${weights[@]}"
}

prepare_execute_config() {
  EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/${BENCHMARK}.xml"

  if [[ -z "${QUERY_SET}" ]]; then
    return
  fi

  if [[ "${BENCHMARK}" != "tpch" ]]; then
    echo "Query subset is supported only for TPC-H." >&2
    exit 2
  fi

  local base_config="${CONFIG_DIR}/${BENCHMARK}.xml"
  local safe_selector="${QUERY_SET,,}"
  safe_selector="${safe_selector//[^a-z0-9,]/-}"
  GENERATED_CONFIG_LOCAL="${CONFIG_DIR}/.${BENCHMARK}-${safe_selector}.xml"

  local weights
  weights="$(tpch_query_weights)"
  sed "s#<weights>.*</weights>#      <weights>${weights}</weights>#" "${base_config}" > "${GENERATED_CONFIG_LOCAL}"
  EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/$(basename "${GENERATED_CONFIG_LOCAL}")"
  trap cleanup_generated_config EXIT
}

start_spark() {
  compose up --build -d spark-master spark-worker-1 spark-worker-2
}

start_thrift() {
  compose --profile benchbase up --build -d spark-thrift-server
  for _ in $(seq 1 90); do
    if compose exec -T spark-thrift-server bash -c "</dev/tcp/127.0.0.1/10000" >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  echo "Spark Thrift did not become ready on port 10000." >&2
  exit 1
}

stop_thrift() {
  compose stop spark-thrift-server >/dev/null 2>&1 || true
}

start_flight_servers() {
  compose up --build -d "${FLIGHT_SERVER_SERVICES[@]}"
}

wait_flight_servers() {
  local service
  for service in "${FLIGHT_SERVER_SERVICES[@]}"; do
    local ready=0
    for _ in $(seq 1 90); do
      if compose exec -T "${service}" bash -c "</dev/tcp/${service}/32010" >/dev/null 2>&1; then
        echo "${service} ready"
        ready=1
        break
      fi
      sleep 2
    done
    if (( ready == 0 )); then
      echo "${service} did not become ready on port 32010." >&2
      exit 1
    fi
  done
}

register_flight_tables() {
  compose --profile benchbase run --rm --entrypoint bash spark-benchmark-publisher \
    -c "rm -rf /spark-warehouse/metastore_db && echo 'Cleaned stale Derby metastore'"
  compose --profile benchbase run --rm spark-benchmark-publisher
}

generate_parquet_data() {
  compose --profile benchbase build duckdb-benchmark-generator
  compose --profile benchbase run --rm duckdb-benchmark-generator
}

benchbase_execute() {
  prepare_results_dir
  compose --profile benchbase build benchbase-spark
  compose --profile benchbase run --rm benchbase-spark \
    -b "${BENCHMARK}" \
    -c "${EXEC_CONFIG_IN_CONTAINER}" \
    --create=false \
    --load=false \
    --execute=true \
    -d "${RESULTS_IN_CONTAINER}"
}

prepare_stack() {
  generate_parquet_data
  start_flight_servers
  wait_flight_servers
  start_spark
  register_flight_tables
  start_thrift
}

normalize_benchmark

if [[ "${BENCHMARK}" != "tpch" && "${BENCHMARK}" != "tpcds" ]]; then
  usage
  exit 2
fi

if [[ "${MODE}" == "smoke" && "${BENCHMARK}" == "tpch" && -z "${QUERY_SET}" ]]; then
  QUERY_SET="q6"
fi

configure_cluster
prepare_execute_config

case "${MODE}" in
  smoke|fresh)
    compose down -v
    prepare_stack
    benchbase_execute
    ;;
  prepare)
    compose down -v
    prepare_stack
    ;;
  run)
    start_thrift
    benchbase_execute
    ;;
  report)
    python3 "${SCRIPT_DIR}/visualize-results.py"
    ;;
  down)
    compose --profile benchbase down -v
    ;;
  logs)
    compose --profile benchbase logs -f
    ;;
  help|--help|-h)
    usage
    ;;
  *)
    usage
    exit 2
    ;;
esac
