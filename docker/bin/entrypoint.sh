#!/usr/bin/env bash
set -euo pipefail

mode="${1:-server}"
shift || true

DEFAULT_SERVER_JAVA_OPTS=(
  --add-modules java.se
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens java.management/sun.management=ALL-UNNAMED
  --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
  -Dio.netty.tryReflectionSetAccessible=true
)

DEFAULT_SPARK_JAVA_OPTIONS="${DEFAULT_SPARK_JAVA_OPTIONS:-\
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=java.base/sun.security.action=ALL-UNNAMED \
-Dio.netty.tryReflectionSetAccessible=true}"

export SPARK_DAEMON_JAVA_OPTS="${SPARK_DAEMON_JAVA_OPTS:-${DEFAULT_SPARK_JAVA_OPTIONS}}"

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local timeout_seconds="${3:-60}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for ${host}:${port}" >&2
  return 1
}

expose_app_jar_to_spark_driver() {
  export SPARK_CLASSPATH="${APP_JAR}${SPARK_CLASSPATH:+:${SPARK_CLASSPATH}}"
  export SPARK_DIST_CLASSPATH="${APP_JAR}${SPARK_DIST_CLASSPATH:+:${SPARK_DIST_CLASSPATH}}"
}

spark_submit_common() {
  local app_file="$1"
  shift
  wait_for_tcp "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 120
  expose_app_jar_to_spark_driver
  exec "${SPARK_HOME}/bin/spark-submit" \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --jars "${APP_JAR}" \
    --driver-class-path "${APP_JAR}" \
    --driver-java-options "${SPARK_DRIVER_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}" \
    --conf "spark.executor.extraJavaOptions=${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}" \
    --conf "spark.driver.extraClassPath=${APP_JAR}" \
    --conf "spark.executor.extraClassPath=${APP_JAR}" \
    --conf "spark.driver.userClassPathFirst=true" \
    --conf "spark.executor.userClassPathFirst=true" \
    --conf "spark.driver.bindAddress=0.0.0.0" \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST:-$(hostname -f)}" \
    --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-8}" \
    --conf "spark.sql.catalogImplementation=hive" \
    --conf "spark.sql.warehouse.dir=${SPARK_WAREHOUSE_DIR:-/spark-warehouse}" \
    --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=${SPARK_METASTORE_DB:-/spark-warehouse/metastore_db};create=true" \
    "${app_file}" \
    "$@"
}

spark_common_conf=(
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}"
  --jars "${APP_JAR}"
  --driver-class-path "${APP_JAR}"
  --driver-java-options "${SPARK_DRIVER_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}"
  --conf "spark.executor.extraJavaOptions=${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}"
  --conf "spark.driver.extraClassPath=${APP_JAR}"
  --conf "spark.executor.extraClassPath=${APP_JAR}"
  --conf "spark.driver.userClassPathFirst=true"
  --conf "spark.executor.userClassPathFirst=true"
  --conf "spark.driver.bindAddress=0.0.0.0"
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-$(hostname -f)}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-8}"
  --conf "spark.sql.catalogImplementation=hive"
  --conf "spark.sql.warehouse.dir=${SPARK_WAREHOUSE_DIR:-/spark-warehouse}"
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=${SPARK_METASTORE_DB:-/spark-warehouse/metastore_db};create=true"
)

case "${mode}" in
  server)
    java_opts=("${DEFAULT_SERVER_JAVA_OPTS[@]}")
    if [[ -n "${JAVA_OPTS:-}" ]]; then
      read -r -a extra_java_opts <<< "${JAVA_OPTS}"
      java_opts+=("${extra_java_opts[@]}")
    fi
    exec java "${java_opts[@]}" \
      -cp "${APP_JAR}:${SPARK_HOME}/jars/*" \
      net.surpin.data.arrowflight.server.HadoopArrowFlightServer \
      --data-dir "${FLIGHT_DATA_DIR:-/data/parquet}" \
      --port "${FLIGHT_PORT:-32010}" \
      --hosts "${FLIGHT_HOSTS:-flight-server-1,flight-server-2,flight-server-3}" \
      --localhost "${FLIGHT_LOCALHOST:-$(hostname -f)}" \
      --hazelcast-port "${HAZELCAST_PORT:-5701}" \
      "$@"
    ;;
  spark-master)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST:-spark-master}" \
      --port "${SPARK_MASTER_PORT:-7077}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}" \
      "$@"
    ;;
  spark-worker)
    wait_for_tcp "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 120
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --cores "${SPARK_WORKER_CORES:-2}" \
      --memory "${SPARK_WORKER_MEMORY:-2g}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
      "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
      "$@"
    ;;
  publish-benchmark-data)
    spark_submit_common "${APP_HOME}/spark/publish_benchbase_tables.py" "$@"
    ;;
  spark-thrift-server)
    wait_for_tcp "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 120
    expose_app_jar_to_spark_driver
    exec "${SPARK_HOME}/bin/spark-submit" \
      "${spark_common_conf[@]}" \
      --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
      "${SPARK_HOME}/jars/spark-hive-thriftserver_2.12-3.5.1.jar" \
      --hiveconf "hive.server2.thrift.bind.host=${SPARK_THRIFT_BIND_HOST:-0.0.0.0}" \
      --hiveconf "hive.server2.thrift.port=${SPARK_THRIFT_PORT:-10000}" \
      "$@"
    ;;
  *)
    exec "${mode}" "$@"
    ;;
esac
