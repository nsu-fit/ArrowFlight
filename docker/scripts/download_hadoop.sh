#!/bin/bash
set -e

HADOOP_VERSION=${HADOOP_VERSION:-3.3.6}
HADOOP_PACKAGE="hadoop-${HADOOP_VERSION}"

if [ -d "/opt/${HADOOP_PACKAGE}" ]; then
    echo "==> Hadoop already downloaded, skipping."
    exit 0
fi

echo "==> Downloading Hadoop ${HADOOP_VERSION}..."

apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

for url in "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}.tar.gz" \
           "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}.tar.gz"; do
    if curl -fL --retry 5 --retry-all-errors --retry-delay 10 -o /tmp/hadoop.tar.gz "$url"; then
        tar -xzf /tmp/hadoop.tar.gz -C /opt
        rm /tmp/hadoop.tar.gz
        ln -sf "/opt/${HADOOP_PACKAGE}" /opt/hadoop
        echo "    Hadoop installed."
        exit 0
    fi
done

echo "Error: failed to download Hadoop" >&2
exit 1
