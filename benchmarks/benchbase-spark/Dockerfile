ARG BENCHBASE_IMAGE=benchbase.azurecr.io/benchbase:latest
FROM ${BENCHBASE_IMAGE}

ARG HIVE_JDBC_VERSION=2.3.9

USER root
RUN mkdir -p /benchbase/lib
ADD https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/${HIVE_JDBC_VERSION}/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar /benchbase/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar
RUN chmod 0644 /benchbase/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar
COPY benchbase-entrypoint.sh /benchbase-entrypoint.sh
RUN chmod +x /benchbase-entrypoint.sh

ENTRYPOINT ["/benchbase-entrypoint.sh"]
