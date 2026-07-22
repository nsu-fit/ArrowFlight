#!/bin/bash
set -e

SPARK_VERSION=${SPARK_VERSION:-3.5.9}
SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop3"

if [ -d "/opt/${SPARK_PACKAGE}" ]; then
    echo "==> Spark already downloaded, skipping."
    exit 0
fi

echo "==> Downloading Spark ${SPARK_VERSION}..."

apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

for url in "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
           "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz"; do
    if curl -fL --retry 5 --retry-all-errors --retry-delay 10 -o /tmp/spark.tgz "$url"; then
        tar -xzf /tmp/spark.tgz -C /opt
        rm /tmp/spark.tgz
        ln -sf "/opt/${SPARK_PACKAGE}" /opt/spark
        echo "    Spark installed."
        exit 0
    fi
done

echo "Error: failed to download Spark" >&2
exit 1
