# syntax=docker/dockerfile:1

ARG UBUNTU_MIRROR=http://mirror.yandex.ru/ubuntu

# Stage 1: Download Spark
FROM eclipse-temurin:21-jre-jammy AS spark-downloader
ARG UBUNTU_MIRROR
WORKDIR /opt
COPY docker/scripts/configure_apt_mirror.sh docker/scripts/download_spark.sh ./
RUN chmod +x configure_apt_mirror.sh download_spark.sh \
    && UBUNTU_MIRROR="${UBUNTU_MIRROR}" ./download_spark.sh

# Stage 2: Download Hadoop
FROM eclipse-temurin:21-jre-jammy AS hadoop-downloader
ARG UBUNTU_MIRROR
WORKDIR /opt
COPY docker/scripts/configure_apt_mirror.sh docker/scripts/download_hadoop.sh ./
RUN chmod +x configure_apt_mirror.sh download_hadoop.sh \
    && UBUNTU_MIRROR="${UBUNTU_MIRROR}" ./download_hadoop.sh

# Stage 3: Build Java application
FROM maven:3.9.9-eclipse-temurin-21 AS java-builder
WORKDIR /src
COPY pom.xml checkstyle.xml spotbugs-exclude.xml ./
RUN mvn -q dependency:go-offline dependency:resolve-plugins
COPY src/ ./src/
RUN mvn -q -DskipTests package

# Stage 4: Final runtime image
FROM eclipse-temurin:21-jre-jammy
ARG UBUNTU_MIRROR

ENV APP_HOME=/opt/arrowflight
ENV APP_JAR=${APP_HOME}/hadoop-arrow-flight-1.0-SNAPSHOT.jar
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# Copy artifacts from previous stages
COPY --from=spark-downloader /opt/spark /opt/spark
COPY --from=hadoop-downloader /opt/hadoop /opt/hadoop
COPY --from=java-builder /src/target/hadoop-arrow-flight-1.0-SNAPSHOT.jar ${APP_JAR}

COPY docker/scripts/configure_apt_mirror.sh /usr/local/bin/configure-apt-mirror
RUN chmod +x /usr/local/bin/configure-apt-mirror \
    && UBUNTU_MIRROR="${UBUNTU_MIRROR}" /usr/local/bin/configure-apt-mirror \
    && apt-get update \
    && apt-get install -y --no-install-recommends bash curl procps tini python3 \
    && rm -rf /var/lib/apt/lists/* \
    && rm /usr/local/bin/configure-apt-mirror

ENV LD_LIBRARY_PATH=/opt/hadoop/lib/native:/opt/java/openjdk/lib/server
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}

WORKDIR ${APP_HOME}
COPY docker/bin/ ./bin/
COPY docker/hadoop/ ${HADOOP_CONF_DIR}/
COPY docker/spark/ ./spark/
RUN chmod +x ./bin/*.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/arrowflight/bin/entrypoint.sh"]
CMD ["server"]
