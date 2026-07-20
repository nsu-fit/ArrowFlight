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
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /work/queries && \
    for n in $(seq -w 1 99); do \
      url="https://raw.githubusercontent.com/duckdb/duckdb/main/extension/tpcds/dsdgen/queries/${n}.sql"; \
      echo "Downloading ${url}"; \
      curl -sfL --retry 3 --retry-delay 2 -o "/work/queries/${n}.sql" "${url}"; \
    done && \
    python /work/generate-procedures.py && \
    ls -1 /work/src/com/oltpbenchmark/benchmarks/tpcds/procedures/ | wc -l

# -----------------------------------------------------------------------------
# Stage 2: Clone BenchBase and patch it with the TPC-DS plugin.
# -----------------------------------------------------------------------------
FROM ${MAVEN_IMAGE} AS builder

WORKDIR /src
RUN git clone --depth 1 https://github.com/cmu-db/benchbase.git .

# Patch plugin.xml to register TPC-DS.
COPY benchmarks/benchbase-spark/tpcds/plugin.xml config/plugin.xml

# Patch TPCDSWorker (replace the upstream stub).
COPY benchmarks/benchbase-spark/tpcds/src/com/oltpbenchmark/benchmarks/tpcds/TPCDSWorker.java \
     src/main/java/com/oltpbenchmark/benchmarks/tpcds/TPCDSWorker.java

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
WORKDIR ${BENCHBASE_HOME}

COPY --from=builder /src/target/benchbase.jar benchbase.jar
COPY --from=builder /src/target/lib/ lib/
COPY --from=builder /src/config/ config/

RUN mkdir -p results

ENTRYPOINT ["java", "-jar", "benchbase.jar"]
