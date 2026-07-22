#!/bin/bash
set -e

DUCKDB_HDFS_VERSION=${DUCKDB_HDFS_VERSION:-1.4.1}
INSTALL_DIR="/opt/duckdb-extensions"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if the extension is already compiled to skip unnecessary work
if [ -d "${INSTALL_DIR}" ] && [ -f "${INSTALL_DIR}/hadoopfs.duckdb_extension" ]; then
    echo "==> DuckDB HDFS extension already built, skipping."
    exit 0
fi

echo "==> Building DuckDB HDFS extension (version ${DUCKDB_HDFS_VERSION})..."

# Install build dependencies
# yasm is required for libhdfs3 assembly optimizations
"${SCRIPT_DIR}/configure_apt_mirror.sh"
apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git ninja-build yasm curl ca-certificates pkg-config \
    libssl-dev libkrb5-dev uuid-dev \
    libgsasl7-dev libxml2-dev libboost-all-dev libprotobuf-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# 1. Build libhdfs3 from ClickHouse fork (actively maintained, compiles cleanly)
echo "==> Building libhdfs3 (ClickHouse fork)..."
cd /tmp
git clone --depth 1 https://github.com/ClickHouse/libhdfs3.git /tmp/libhdfs3
mkdir -p /tmp/libhdfs3/build && cd /tmp/libhdfs3/build
cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local
ninja -j"$(nproc)" libhdfs3-shared
ninja install
cd /tmp
rm -rf /tmp/libhdfs3

# 2. Build DuckDB HDFS extension
echo "==> Cloning duckdb-hdfs repository..."
git clone --depth 1 --branch "v${DUCKDB_HDFS_VERSION}" https://github.com/vincent-chang/duckdb-hdfs.git /duckdb-hdfs
cd /duckdb-hdfs

# CRITICAL: Initialize submodules and explicitly checkout the matching DuckDB version
# This prevents ABI mismatches between the extension and the DuckDB core
echo "==> Syncing DuckDB core to version v${DUCKDB_HDFS_VERSION}..."
git submodule update --init --depth 1 duckdb extension-ci-tools
cd duckdb
git fetch --depth 1 origin tag "v${DUCKDB_HDFS_VERSION}" 2>/dev/null || true
git checkout "v${DUCKDB_HDFS_VERSION}" 2>/dev/null || true
cd /duckdb-hdfs

echo "==> Running DuckDB build..."
export USE_NINJA=1
make -j"$(nproc)" release

# 3. Diagnostics and Installation
echo "==> Build completed. Checking output..."

# FIX: Remove stale file/dir if it exists from a previous failed run
if [ -e "${INSTALL_DIR}" ]; then
    echo "==> Removing stale path at ${INSTALL_DIR}..."
    rm -rf "${INSTALL_DIR}"
fi

mkdir -p "${INSTALL_DIR}"

# Copy the extension and the compiled libhdfs3 shared libraries
echo "==> Copying artifacts to ${INSTALL_DIR}..."
cp -v build/release/extension/hadoopfs/hadoopfs.duckdb_extension "${INSTALL_DIR}/"
cp -v /usr/local/lib/libhdfs3.so* "${INSTALL_DIR}/"

# Verify files were copied
if [ ! -f "${INSTALL_DIR}/hadoopfs.duckdb_extension" ]; then
    echo "ERROR: Extension file not found after build!" >&2
    find build/release -type f -name "*.duckdb_extension" >&2 || true
    exit 1
fi

echo "==> Successfully copied:"
ls -lh "${INSTALL_DIR}/"

# Clean up source code to minimize Docker layer size
cd /tmp
rm -rf /duckdb-hdfs

echo "==> DuckDB HDFS extension v${DUCKDB_HDFS_VERSION} built successfully."
