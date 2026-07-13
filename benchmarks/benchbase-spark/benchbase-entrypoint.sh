#!/usr/bin/env bash
set -euo pipefail

classpath=""
for root in /benchbase /app /opt/benchbase; do
  if [[ -d "${root}" ]]; then
    while IFS= read -r jar; do
      if [[ -z "${classpath}" ]]; then
        classpath="${jar}"
      else
        classpath="${classpath}:${jar}"
      fi
    done < <(find "${root}" -type f -name '*.jar' | sort)

    for dir in "${root}/target/classes" "${root}/build/classes/java/main" "${root}/classes"; do
      if [[ -d "${dir}" ]]; then
        classpath="${classpath}:${dir}"
      fi
    done
  fi
done

if [[ -z "${classpath}" ]]; then
  echo "No BenchBase jars found in image." >&2
  exit 1
fi

exec java ${JAVA_OPTS:-} -cp "${classpath}" com.oltpbenchmark.DBWorkload "$@"
