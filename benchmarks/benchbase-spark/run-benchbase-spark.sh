#!/usr/bin/env bash
set -euo pipefail

BENCHMARK="${1:-tpch}"
MODE="${2:-smoke}"
QUERY_SET="${3:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.yml"
RESULTS_ROOT="${SCRIPT_DIR}/results"
RESULTS_RUN_ID="${BENCHBASE_RESULTS_ID:-}"
RESULTS_DIR="${RESULTS_ROOT}"
CONFIG_DIR="${SCRIPT_DIR}/config"
RESULTS_IN_CONTAINER="/benchbase/results"
GENERATED_CONFIG_LOCAL=""
EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/${BENCHMARK}.xml"
FLIGHT_SERVER_SERVICES=()
BENCHBASE_TIME_SECONDS="${BENCHBASE_TIME_SECONDS:-}"
BENCHBASE_TERMINALS="${BENCHBASE_TERMINALS:-}"
BENCHBASE_RATE="${BENCHBASE_RATE:-unlimited}"

usage() {
  cat >&2 <<'EOF'
Usage: bash benchmarks/benchbase-spark/run-benchbase-spark.sh <benchmark> <mode> [queries]

Benchmarks:
  tpch
  tpcds

Modes:
  smoke    reset volumes, DuckDB generate Parquet, register Flight, verify Spark, execute small query set
  graph    reset volumes, run a timed TPC-H query set so the HTML report has chart points
  fresh    reset volumes, DuckDB generate Parquet, register Flight, verify Spark, execute benchmark
  prepare  reset volumes, DuckDB generate Parquet, register Flight, verify Spark, start Spark Thrift
  run      execute BenchBase against already prepared Spark Flight tables
  report   build latest HTML report
  down     stop stack and remove Docker volumes
  logs     follow compose logs

Queries:
  TPC-H only: q6, q1,q6,q14, 1,6,14
EOF
}

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

reset_stack() {
  compose --profile benchbase down -v --remove-orphans
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
  echo "BENCHMARK_SCALE_FACTOR=${BENCHMARK_SCALE_FACTOR}"
}

prepare_results_dir() {
  if [[ -z "${RESULTS_RUN_ID}" ]]; then
    local query_label="${QUERY_SET:-all}"
    query_label="${query_label,,}"
    query_label="${query_label//[^a-z0-9,]/-}"
    RESULTS_RUN_ID="${BENCHMARK}-${MODE}-${query_label}-$(date +%Y%m%d-%H%M%S)"
  fi

  RESULTS_DIR="${RESULTS_ROOT}/${RESULTS_RUN_ID}"
  RESULTS_IN_CONTAINER="/benchbase/results/${RESULTS_RUN_ID}"
  mkdir -p "${RESULTS_DIR}"
  chmod a+rwx "${RESULTS_DIR}" 2>/dev/null || true
  echo "Results directory: ${RESULTS_DIR}"
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

  if [[ -z "${QUERY_SET}" && -z "${BENCHBASE_TIME_SECONDS}" && -z "${BENCHBASE_TERMINALS}" ]]; then
    return
  fi

  if [[ -n "${QUERY_SET}" && "${BENCHMARK}" != "tpch" ]]; then
    echo "Query subset is supported only for TPC-H." >&2
    exit 2
  fi

  local base_config="${CONFIG_DIR}/${BENCHMARK}.xml"
  local safe_selector="${QUERY_SET:-all}"
  safe_selector="${safe_selector,,}"
  safe_selector="${safe_selector//[^a-z0-9,]/-}"
  GENERATED_CONFIG_LOCAL="${CONFIG_DIR}/.${BENCHMARK}-${safe_selector}-${BENCHBASE_TIME_SECONDS:-serial}.xml"
  cp "${base_config}" "${GENERATED_CONFIG_LOCAL}"

  if [[ -n "${QUERY_SET}" ]]; then
    local weights
    weights="$(tpch_query_weights)"
    sed -i "s#<weights>.*</weights>#      <weights>${weights}</weights>#" "${GENERATED_CONFIG_LOCAL}"
  fi

  if [[ -n "${BENCHBASE_TERMINALS}" ]]; then
    sed -i "s#<terminals>.*</terminals>#  <terminals>${BENCHBASE_TERMINALS}</terminals>#" "${GENERATED_CONFIG_LOCAL}"
  fi

  if [[ -n "${BENCHBASE_TIME_SECONDS}" ]]; then
    sed -i "s#<serial>.*</serial>#      <serial>false</serial>#" "${GENERATED_CONFIG_LOCAL}"
    sed -i "s#<rate>.*</rate>#      <rate>${BENCHBASE_RATE}</rate>#" "${GENERATED_CONFIG_LOCAL}"
    sed -i "/<serial>false<\/serial>/a\\      <time>${BENCHBASE_TIME_SECONDS}</time>\\n      <warmup>0</warmup>" "${GENERATED_CONFIG_LOCAL}"
  fi

  EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/$(basename "${GENERATED_CONFIG_LOCAL}")"
  trap cleanup_generated_config EXIT
}

wait_service_healthy() {
  local service="$1"
  local timeout_seconds="${2:-180}"
  local deadline=$((SECONDS + timeout_seconds))
  local container_id=""
  local status=""

  while (( SECONDS < deadline )); do
    container_id="$(compose --profile benchbase ps -q "${service}" 2>/dev/null || true)"
    if [[ -n "${container_id}" ]]; then
      status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}" 2>/dev/null || true)"
      if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
        echo "${service} ready"
        return
      fi
      if [[ "${status}" == "exited" || "${status}" == "dead" ]]; then
        break
      fi
    fi
    sleep 2
  done

  echo "${service} did not become ready. Last status: ${status:-unknown}." >&2
  compose --profile benchbase logs --tail=100 "${service}" >&2 || true
  exit 1
}

start_spark() {
  compose up --build -d spark-master spark-worker-1 spark-worker-2
}

start_thrift() {
  compose --profile benchbase up --build --force-recreate -d spark-thrift-server
  wait_service_healthy spark-thrift-server 180
}

start_flight_servers() {
  compose up --build -d "${FLIGHT_SERVER_SERVICES[@]}"
}

wait_flight_servers() {
  local service
  for service in "${FLIGHT_SERVER_SERVICES[@]}"; do
    wait_service_healthy "${service}" 180
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

verify_spark_flight() {
  local table
  case "${BENCHMARK}" in
    tpch) table="nation" ;;
    tpcds) table="date_dim" ;;
    *) table="" ;;
  esac

  if [[ -z "${table}" ]]; then
    return
  fi

  echo "Verifying Spark Thrift can read ${BENCHMARK}.${table} through Flight"
  compose --profile benchbase exec -T spark-thrift-server bash -lc \
    "printf '%s\n' 'SELECT COUNT(*) AS row_count FROM ${BENCHMARK}.${table};' > /tmp/verify-flight.sql && /opt/spark/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/${BENCHMARK}' -n benchbase -f /tmp/verify-flight.sql"
}

fail_on_benchbase_sql_errors() {
  local log_file="$1"
  if awk '
    /Unexpected SQL Errors:/ { section = 1; next }
    /Unknown Status Transactions:/ { section = 0 }
    section && /\[[[:space:]]*[1-9][0-9]*\]/ { bad = 1 }
    END { exit bad ? 0 : 1 }
  ' "${log_file}"; then
    echo "BenchBase reported unexpected SQL errors. See ${log_file}" >&2
    exit 1
  fi
}

benchbase_execute() {
  prepare_results_dir
  compose --profile benchbase build benchbase-spark

  local log_file="${RESULTS_DIR}/last-${BENCHMARK}.log"
  set +e
  compose --profile benchbase run --rm benchbase-spark \
    -b "${BENCHMARK}" \
    -c "${EXEC_CONFIG_IN_CONTAINER}" \
    --create=false \
    --load=false \
    --execute=true \
    -d "${RESULTS_IN_CONTAINER}" 2>&1 | tee "${log_file}"
  local benchbase_status="${PIPESTATUS[0]}"
  set -e

  if (( benchbase_status != 0 )); then
    exit "${benchbase_status}"
  fi
  fail_on_benchbase_sql_errors "${log_file}"
}

prepare_stack() {
  generate_parquet_data
  start_flight_servers
  wait_flight_servers
  start_spark
  register_flight_tables
  start_thrift
  verify_spark_flight
}

normalize_benchmark

if [[ "${BENCHMARK}" == "help" || "${BENCHMARK}" == "--help" || "${BENCHMARK}" == "-h" || "${MODE}" == "help" || "${MODE}" == "--help" || "${MODE}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "${BENCHMARK}" != "tpch" && "${BENCHMARK}" != "tpcds" ]]; then
  usage
  exit 2
fi

if [[ "${MODE}" == "smoke" && "${BENCHMARK}" == "tpch" && -z "${QUERY_SET}" ]]; then
  QUERY_SET="q6"
fi

if [[ "${MODE}" == "graph" ]]; then
  if [[ "${BENCHMARK}" != "tpch" ]]; then
    echo "graph mode is currently supported only for TPC-H." >&2
    exit 2
  fi
  QUERY_SET="${QUERY_SET:-q6}"
  BENCHBASE_TIME_SECONDS="${BENCHBASE_TIME_SECONDS:-30}"
  BENCHBASE_TERMINALS="${BENCHBASE_TERMINALS:-1}"
fi

configure_cluster
prepare_execute_config

case "${MODE}" in
  smoke|fresh|graph)
    reset_stack
    prepare_stack
    benchbase_execute
    ;;
  prepare)
    reset_stack
    prepare_stack
    ;;
  run)
    start_thrift
    verify_spark_flight
    benchbase_execute
    ;;
  report)
    python3 "${SCRIPT_DIR}/visualize-results.py"
    ;;
  down)
    reset_stack
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
