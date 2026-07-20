# syntax=docker/dockerfile:1

ARG MAVEN_IMAGE=maven:3.9.9-eclipse-temurin-21
ARG RUNTIME_IMAGE=eclipse-temurin:21-jre-jammy

# ── build stage: compile Java code ────────────────────────
FROM ${MAVEN_IMAGE} AS build
WORKDIR /src
COPY pom.xml .
COPY checkstyle.xml spotbugs-exclude.xml ./
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

# ── duckdb-hdfs build stage: compile extension from source ──
FROM ubuntu:jammy AS duckdb-hdfs-build
ARG DUCKDB_HDFS_VERSION=1.4.1
ARG LIBHDFS3_VERSION=2.2.30
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git ninja-build curl ca-certificates pkg-config \
    libssl-dev libkrb5-dev uuid-dev \
    libgsasl7-dev libxml2-dev libboost-dev libprotobuf-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "v${LIBHDFS3_VERSION}" \
    https://github.com/vincent-chang/libhdfs3.git /tmp/libhdfs3 \
    && for f in /tmp/libhdfs3/src/proto/*.proto; do \
         protoc --cpp_out=/tmp/libhdfs3/src -I /tmp/libhdfs3/src/proto "$f"; \
       done \
    && sed -i '/^PROTOBUF_GENERATE_CPP(libhdfs3_PROTO_SOURCES/d' /tmp/libhdfs3/src/CMakeLists.txt \
    && sed -i 's/ADD_LIBRARY(libhdfs3-static STATIC ${libhdfs3_SOURCES} ${libhdfs3_PROTO_SOURCES} ${libhdfs3_PROTO_HEADERS})/ADD_LIBRARY(libhdfs3-static STATIC ${libhdfs3_SOURCES})/' /tmp/libhdfs3/src/CMakeLists.txt \
    && sed -i 's/ADD_LIBRARY(libhdfs3-shared SHARED ${libhdfs3_SOURCES} ${libhdfs3_PROTO_SOURCES} ${libhdfs3_PROTO_HEADERS})/ADD_LIBRARY(libhdfs3-shared SHARED ${libhdfs3_SOURCES})/' /tmp/libhdfs3/src/CMakeLists.txt \
    && sed -i '/ADD_CUSTOM_COMMAND/,/^)$/d' /tmp/libhdfs3/src/CMakeLists.txt \
    && mkdir /tmp/libhdfs3/build && cd /tmp/libhdfs3/build \
    && cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
    && ninja -j"$(nproc)" libhdfs3-shared && ninja install \
    && rm -rf /tmp/libhdfs3
RUN git clone --depth 1 --branch "v${DUCKDB_HDFS_VERSION}" \
    https://github.com/vincent-chang/duckdb-hdfs.git /duckdb-hdfs \
    && cd /duckdb-hdfs \
    && git submodule update --init --depth 1 duckdb extension-ci-tools \
    && cd duckdb && git fetch --depth 1 origin tag "v${DUCKDB_HDFS_VERSION}" 2>/dev/null; cd /duckdb-hdfs \
    && make release EXT_RELEASE_FLAGS="-DBUILD_SHELL=FALSE -DBUILD_UNITTESTS=FALSE -DBUILD_BENCHMARKS=FALSE" \
    && mkdir -p /opt/duckdb-hdfs \
    && cp build/release/extension/hadoopfs/hadoopfs.duckdb_extension /opt/duckdb-hdfs/ \
    && cp /usr/local/lib/libhdfs3.so* /opt/duckdb-hdfs/

# ── base stage: heavy dependencies (Spark, Hadoop, DuckDB) ──
# No COPY from context → always cached when versions unchanged.
FROM ${RUNTIME_IMAGE} AS base
ARG SPARK_VERSION=3.5.9
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop3
ARG HADOOP_VERSION=3.3.6
ARG HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}
ARG DUCKDB_HDFS_VERSION=1.4.1

ENV APP_HOME=/opt/arrowflight
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV ARROW_LIBHDFS_DIR=/opt/hadoop/lib/native
ENV DUCKDB_HDFS_HOME=/opt/duckdb-hdfs
ENV DUCKDB_HDFS_EXTENSION=/opt/duckdb-hdfs/hadoopfs.duckdb_extension
ENV DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true
ENV LD_LIBRARY_PATH=/opt/duckdb-hdfs:/opt/hadoop/lib/native:/opt/java/openjdk/lib/server
ENV PATH=/opt/spark/bin:/opt/spark/sbin:/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl procps tini \
        libuuid1 libprotobuf23 libxml2 libkrb5-3 libgsasl7 python3 \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
        for url in \
            "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
            "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz"; do \
          for attempt in 1 2 3 4 5; do \
            rm -f /tmp/spark.tgz; \
            if curl -fL --retry 5 --retry-all-errors --retry-delay 10 --connect-timeout 30 \
                -o /tmp/spark.tgz "${url}" \
                && tar -tzf /tmp/spark.tgz >/dev/null; then \
              break 2; \
            fi; \
            echo "Spark download failed from ${url}, attempt ${attempt}" >&2; \
            sleep 10; \
          done; \
        done; \
        test -s /tmp/spark.tgz; \
        tar -xzf /tmp/spark.tgz -C /opt; \
        rm -f /tmp/spark.tgz; \
        ln -s "/opt/${SPARK_PACKAGE}" "${SPARK_HOME}"

RUN set -eux; \
        for url in \
            "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}.tar.gz" \
            "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}.tar.gz"; do \
          for attempt in 1 2 3 4 5; do \
            rm -f /tmp/hadoop.tgz; \
            if curl -fL --retry 5 --retry-all-errors --retry-delay 10 --connect-timeout 30 \
                -o /tmp/hadoop.tgz "${url}" \
                && tar -tzf /tmp/hadoop.tgz >/dev/null; then \
              break 2; \
            fi; \
            echo "Hadoop download failed from ${url}, attempt ${attempt}" >&2; \
            sleep 10; \
          done; \
        done; \
        test -s /tmp/hadoop.tgz; \
        tar -xzf /tmp/hadoop.tgz -C /opt; \
        rm -f /tmp/hadoop.tgz; \
        ln -s "/opt/${HADOOP_PACKAGE}" "${HADOOP_HOME}"

COPY --from=duckdb-hdfs-build /opt/duckdb-hdfs/ /opt/duckdb-hdfs/

# ── app stage: thin application layer (rebuilt on every code change) ──
FROM base AS app
ENV APP_JAR=/opt/arrowflight/hadoop-arrow-flight-1.0-SNAPSHOT.jar
WORKDIR ${APP_HOME}
COPY --from=build /src/target/hadoop-arrow-flight-1.0-SNAPSHOT.jar ${APP_JAR}
COPY docker/bin/ ./bin/
COPY docker/hadoop/ ${HADOOP_CONF_DIR}/
COPY docker/spark/ ./spark/
RUN ln -sf "${APP_JAR}" "${SPARK_HOME}/jars/hadoop-arrow-flight-1.0-SNAPSHOT.jar" \
    && mkdir -p /var/lib/hadoop-hdfs/name /var/lib/hadoop-hdfs/data /var/lib/hadoop-hdfs/socket \
    && chmod +x ./bin/*.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/arrowflight/bin/entrypoint.sh"]
CMD ["server"]
