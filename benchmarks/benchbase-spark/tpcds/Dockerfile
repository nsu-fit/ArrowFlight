# Builds a BenchBase Docker image with TPC-DS plugin (99 queries from DuckDB).
#
# Build from ArrowFlight repo root:
#   docker build -t benchbase-tpcds:latest \
#     -f benchmarks/benchbase-spark/tpcds/Dockerfile .

ARG PYTHON_IMAGE=python:3.12-slim
ARG MAVEN_IMAGE=maven:3.9.9-eclipse-temurin-21
ARG RUNTIME_IMAGE=eclipse-temurin:21-jre-jammy

# -----------------------------------------------------------------------------
# Stage 1: Download DuckDB TPC-DS queries and generate Java procedure classes.
# -----------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS query-downloader

WORKDIR /work
COPY benchmarks/benchbase-spark/tpcds/generate-procedures.py /work/generate-procedures.py

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && git clone --depth 1 --filter=blob:none --sparse \
        https://github.com/duckdb/duckdb.git /tmp/duckdb \
    && cd /tmp/duckdb \
    && git sparse-checkout set extension/tpcds/dsdgen/queries \
    && mkdir -p /work/queries \
    && cp /tmp/duckdb/extension/tpcds/dsdgen/queries/*.sql /work/queries/ \
    && rm -rf /tmp/duckdb \
    && python /work/generate-procedures.py \
    && ls -1 /work/src/com/oltpbenchmark/benchmarks/tpcds/procedures/ | wc -l

# -----------------------------------------------------------------------------
# Stage 2: Clone BenchBase and patch it with the TPC-DS plugin.
# -----------------------------------------------------------------------------
FROM ${MAVEN_IMAGE} AS builder

WORKDIR /src
# Clone BenchBase, pin to commit before Java 23 upgrade (46fc66f).
RUN git clone https://github.com/cmu-db/benchbase.git . \
    && git checkout 46fc66f

# Patch plugin.xml to register TPC-DS.
COPY benchmarks/benchbase-spark/tpcds/plugin.xml config/plugin.xml

# Patch TPCDSWorker (replace the upstream stub).
COPY benchmarks/benchbase-spark/tpcds/src/com/oltpbenchmark/benchmarks/tpcds/TPCDSWorker.java \
     src/main/java/com/oltpbenchmark/benchmarks/tpcds/TPCDSWorker.java

# Copy TPCDSProcedure base class (not generated — lives in the repo).
COPY benchmarks/benchbase-spark/tpcds/src/com/oltpbenchmark/benchmarks/tpcds/procedures/TPCDSProcedure.java \
     src/main/java/com/oltpbenchmark/benchmarks/tpcds/procedures/TPCDSProcedure.java

# Add generated TPC-DS query procedures (Q1.java ... Q99.java).
COPY --from=query-downloader /work/src/com/oltpbenchmark/benchmarks/tpcds/procedures/ \
     src/main/java/com/oltpbenchmark/benchmarks/tpcds/procedures/

# Build BenchBase with the Postgres profile (JDBC driver for Thrift Server).
RUN mvn -q -DskipTests package -P postgres

# -----------------------------------------------------------------------------
# Stage 3: Runtime image.
# -----------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE}

ARG BENCHBASE_HOME=/benchbase
ARG LOG4J_VERSION=2.24.3
WORKDIR ${BENCHBASE_HOME}

COPY --from=builder /src/target/benchbase-postgres.tgz /tmp/

RUN tar -xzf /tmp/benchbase-postgres.tgz -C /tmp \
    && mv /tmp/benchbase-postgres/benchbase*.jar benchbase.jar \
    && mv /tmp/benchbase-postgres/lib/ lib/ \
    && mv /tmp/benchbase-postgres/config/ config/ \
    && rm -rf /tmp/benchbase-postgres /tmp/benchbase-postgres.tgz

# BenchBase dependencies pull in log4j-api 3.x which conflicts with
# commons-logging. Add log4j-api 2.x with a filename prefix that ensures
# it loads first in the classpath (overriding the incompatible 3.x).
ADD https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/${LOG4J_VERSION}/log4j-api-${LOG4J_VERSION}.jar 000-log4j-api.jar
ADD https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/${LOG4J_VERSION}/log4j-core-${LOG4J_VERSION}.jar 000-log4j-core.jar

RUN mkdir -p results

ENTRYPOINT ["java", "-jar", "benchbase.jar"]
