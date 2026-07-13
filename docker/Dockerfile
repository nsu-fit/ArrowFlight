# syntax=docker/dockerfile:1

ARG MAVEN_IMAGE=maven:3.9.9-eclipse-temurin-21
ARG RUNTIME_IMAGE=eclipse-temurin:21-jre-jammy

FROM ${MAVEN_IMAGE} AS build
WORKDIR /src
COPY pom.xml .
COPY checkstyle.xml spotbugs-exclude.xml ./
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM ${RUNTIME_IMAGE}
ARG SPARK_VERSION=3.5.1
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop3

ENV APP_HOME=/opt/arrowflight
ENV APP_JAR=/opt/arrowflight/hadoop-arrow-flight-1.0-SNAPSHOT.jar
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl procps python3 tini \
    && rm -rf /var/lib/apt/lists/* \
    && set -eux; \
        for url in \
            "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
            "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz"; do \
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

WORKDIR ${APP_HOME}
COPY --from=build /src/target/hadoop-arrow-flight-1.0-SNAPSHOT.jar ${APP_JAR}
COPY docker/bin/ ./bin/
COPY docker/spark/ ./spark/
RUN ln -sf "${APP_JAR}" "${SPARK_HOME}/jars/hadoop-arrow-flight-1.0-SNAPSHOT.jar" \
    && chmod +x ./bin/*.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/arrowflight/bin/entrypoint.sh"]
CMD ["server"]
