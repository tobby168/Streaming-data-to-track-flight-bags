FROM flink:1.18

USER root

ARG FLINK_VERSION=1.18.0
ARG FLINK_KAFKA_CONNECTOR_VERSION=3.2.0-1.18
ARG FLINK_JDBC_CONNECTOR_VERSION=3.2.0-1.18
ARG CLICKHOUSE_JDBC_VERSION=0.6.3
ARG MYSQL_CONNECTOR_VERSION=8.3.0
ARG MARIADB_CONNECTOR_VERSION=3.3.3

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv python3-dev python-is-python3 curl ca-certificates \
        openjdk-11-jdk-headless build-essential zip \
    && rm -rf /var/lib/apt/lists/*

RUN JDK_DIR="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" \
    && if [ -d "${JDK_DIR}/include" ]; then \
        rm -rf /opt/java/openjdk/include && ln -s "${JDK_DIR}/include" /opt/java/openjdk/include; \
    fi

RUN pip install --no-cache-dir "apache-flink==${FLINK_VERSION}" \
    && mkdir -p /opt/flink/lib \
    && curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
        -o "/opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
    && curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${FLINK_JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar" \
        -o "/opt/flink/lib/flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar" \
    && curl -fsSL "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar" \
        -o "/opt/flink/lib/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar" \
    && curl -fsSL "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
        -o "/opt/flink/lib/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
    && curl -fsSL "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/${MARIADB_CONNECTOR_VERSION}/mariadb-java-client-${MARIADB_CONNECTOR_VERSION}.jar" \
        -o "/opt/flink/lib/mariadb-java-client-${MARIADB_CONNECTOR_VERSION}.jar"

COPY flink-jobs /opt/flink/jobs
RUN cd /opt/flink/jobs/flink_jobs && zip -qr /opt/flink/jobs/flink_jobs.zip .

ENV PYTHONPATH=/opt/flink/jobs \
    PYFLINK_CLIENT_EXECUTABLE=python

USER flink
