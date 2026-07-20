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

# ---- download stage (cached until ARGs change) ----
FROM ${RUNTIME_IMAGE} AS download
ARG SPARK_VERSION=3.5.1
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop3
ARG HADOOP_VERSION=3.3.6
ARG HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    for url in \
        "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz"; do \
      rm -f /tmp/spark.tgz; \
      if curl -fL --retry 5 --retry-all-errors --retry-delay 10 --connect-timeout 30 \
          --continue-at - -o /tmp/spark.tgz "${url}" \
          && tar -tzf /tmp/spark.tgz >/dev/null; then \
        break; \
      fi; \
      echo "Spark download failed from ${url}" >&2; \
      sleep 10; \
    done; \
    test -s /tmp/spark.tgz; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    rm -f /tmp/spark.tgz

RUN set -eux; \
    for url in \
        "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}.tar.gz" \
        "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}.tar.gz"; do \
      rm -f /tmp/hadoop.tgz; \
      if curl -fL --retry 5 --retry-all-errors --retry-delay 10 --connect-timeout 30 \
          --continue-at - -o /tmp/hadoop.tgz "${url}" \
          && tar -tzf /tmp/hadoop.tgz >/dev/null; then \
        break; \
      fi; \
      echo "Hadoop download failed from ${url}" >&2; \
      sleep 10; \
    done; \
    test -s /tmp/hadoop.tgz; \
    tar -xzf /tmp/hadoop.tgz -C /opt; \
    rm -f /tmp/hadoop.tgz

# ---- Arrow C++ JNI build with HDFS support ----
FROM eclipse-temurin:21-jdk-jammy AS arrow-build
ARG HADOOP_VERSION=3.3.6
ARG HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}
ARG ARROW_VERSION=18.0.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build \
    curl ca-certificates \
    libssl-dev libcurl4-openssl-dev \
    libboost-dev libboost-filesystem-dev \
    libz-dev liblz4-dev libbrotli-dev \
    libre2-dev libsnappy-dev libzstd-dev \
    libutf8proc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=download /opt/${HADOOP_PACKAGE} /opt/${HADOOP_PACKAGE}
ENV HADOOP_HOME=/opt/${HADOOP_PACKAGE}
ENV PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig

RUN mkdir -p /arrow-source && \
    curl -fsSL https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz | \
    tar xz -C /arrow-source --strip-components=1 && \
    cd /arrow-source/cpp && \
    cmake -S . -B build -GNinja \
        -DCMAKE_BUILD_TYPE=Release \
        -DARROW_HDFS=ON \
        -DARROW_DATASET=ON \
        -DARROW_JNI=ON \
        -DARROW_PARQUET=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_CSV=OFF \
        -DARROW_JSON=OFF \
        -DARROW_FLIGHT=OFF \
        -DARROW_GANDIVA=OFF \
        -DARROW_ORC=OFF \
        -DARROW_FILESYSTEM=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_TESTING=OFF \
        -DARROW_BUILD_TESTS=OFF \
        -DARROW_DEPENDENCY_SOURCE=AUTO \
    && cmake --build build --target arrow_dataset_jni -- -j$(nproc)

# ---- final stage ----
FROM ${RUNTIME_IMAGE}

ARG ARROW_VERSION=18.0.0
ARG SPARK_VERSION=3.5.1
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop3
ARG HADOOP_VERSION=3.3.6
ARG HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}

ENV APP_HOME=/opt/arrowflight
ENV APP_JAR=/opt/arrowflight/hadoop-arrow-flight-1.0-SNAPSHOT.jar
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl procps python3 tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=download /opt/${SPARK_PACKAGE} /opt/${SPARK_PACKAGE}
RUN ln -s "/opt/${SPARK_PACKAGE}" "${SPARK_HOME}"

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV ARROW_LIBHDFS_DIR=/opt/hadoop/lib/native
ENV LD_LIBRARY_PATH=/opt/hadoop/lib/native:/opt/java/openjdk/lib/server
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}

COPY --from=download /opt/${HADOOP_PACKAGE} /opt/${HADOOP_PACKAGE}
RUN ln -s "/opt/${HADOOP_PACKAGE}" "${HADOOP_HOME}"

WORKDIR ${APP_HOME}
COPY --from=build /src/target/hadoop-arrow-flight-1.0-SNAPSHOT.jar ${APP_JAR}

# Replace stock libarrow_dataset_jni.so (HDFS=OFF) with custom HDFS-enabled build
COPY --from=arrow-build /arrow-source/cpp/build/release/libarrow_dataset_jni.so /tmp/
RUN echo "import zipfile, os" > /tmp/replace_so.py && \
    echo "jar = '/opt/arrowflight/hadoop-arrow-flight-1.0-SNAPSHOT.jar'" >> /tmp/replace_so.py && \
    echo "new_so = '/tmp/libarrow_dataset_jni.so'" >> /tmp/replace_so.py && \
    echo "arcname = 'arrow_dataset_jni/x86_64/libarrow_dataset_jni.so'" >> /tmp/replace_so.py && \
    echo "tmp = jar + '.tmp'" >> /tmp/replace_so.py && \
    echo "with zipfile.ZipFile(jar, 'r') as zin:" >> /tmp/replace_so.py && \
    echo "    with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as zout:" >> /tmp/replace_so.py && \
    echo "        for item in zin.infolist():" >> /tmp/replace_so.py && \
    echo "            if item.filename != arcname:" >> /tmp/replace_so.py && \
    echo "                zout.writestr(item, zin.read(item.filename))" >> /tmp/replace_so.py && \
    echo "        zout.write(new_so, arcname)" >> /tmp/replace_so.py && \
    echo "os.replace(tmp, jar)" >> /tmp/replace_so.py && \
    echo "print('Replaced libarrow_dataset_jni.so with HDFS-enabled build')" >> /tmp/replace_so.py && \
    python3 /tmp/replace_so.py && rm -f /tmp/replace_so.py /tmp/libarrow_dataset_jni.so

COPY docker/bin/ ./bin/
COPY docker/hadoop/ ${HADOOP_CONF_DIR}/
COPY docker/spark/ ./spark/
RUN ln -sf "${APP_JAR}" "${SPARK_HOME}/jars/hadoop-arrow-flight-1.0-SNAPSHOT.jar" \
    && mkdir -p /var/lib/hadoop-hdfs/name /var/lib/hadoop-hdfs/data /var/lib/hadoop-hdfs/socket \
    && chmod +x ./bin/*.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/arrowflight/bin/entrypoint.sh"]
CMD ["server"]
