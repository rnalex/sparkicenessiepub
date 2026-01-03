ARG SPARK_VERSION=3.5.2
ARG SCALA_BINARY=2.12
ARG ICEBERG_VERSION=1.10.1
ARG NESSIE_VERSION=0.106.0

FROM python:3.10-slim-bullseye

# Install OpenJDK and utilities
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  openjdk-17-jdk-headless \
  curl \
  tar \
  procps && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Versions for Spark build
ARG SPARK_VERSION

# Download and install Spark
RUN mkdir -p ${SPARK_HOME} && \
  curl -fSL --retry 5 https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark.tgz && \
  tar -xzf spark.tgz -C ${SPARK_HOME} --strip-components=1 && \
  rm spark.tgz

# Versions of Iceberg and Nessie (re-declaring ARGs for use in RUN)
ARG ICEBERG_VERSION
ARG NESSIE_VERSION
ARG SCALA_BINARY

# Download Iceberg and Nessie jars
RUN curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_BINARY}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_BINARY}-${ICEBERG_VERSION}.jar -o ${SPARK_HOME}/jars/iceberg-spark-runtime.jar && \
  curl -sL https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_${SCALA_BINARY}/${NESSIE_VERSION}/nessie-spark-extensions-3.5_${SCALA_BINARY}-${NESSIE_VERSION}.jar -o ${SPARK_HOME}/jars/nessie-spark-extensions.jar && \
  curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -o ${SPARK_HOME}/jars/iceberg-aws-bundle.jar

# Spark Configurations
ENV SPARK_SQL_EXTENSIONS="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
ENV NESSIE_REF="main"
ENV NESSIE_WAREHOUSE="s3://warehouse/"

# We generate a base spark-defaults.conf without dynamic URIs
RUN printf "%s\n" \
  "spark.sql.extensions ${SPARK_SQL_EXTENSIONS}" \
  "spark.sql.catalog.nessie org.apache.iceberg.spark.SparkCatalog" \
  "spark.sql.catalog.nessie.catalog-impl org.apache.iceberg.nessie.NessieCatalog" \
  "spark.sql.catalog.nessie.ref ${NESSIE_REF}" \
  "spark.sql.catalog.nessie.authentication.type NONE" \
  "spark.sql.catalog.nessie.warehouse ${NESSIE_WAREHOUSE}" \
  "spark.sql.catalog.nessie.io-impl org.apache.iceberg.aws.s3.S3FileIO" \
  "spark.hadoop.fs.s3a.access.key admin" \
  "spark.hadoop.fs.s3a.secret.key password" \
  "spark.hadoop.fs.s3a.path.style.access true" \
  "spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem" \
  "spark.hadoop.fs.s3a.endpoint.region us-east-1" \
  "spark.driver.memory 2g" \
  "spark.executor.memory 1g" \
  "spark.driver.maxResultSize 512m" \
  > ${SPARK_HOME}/conf/spark-defaults.conf

ENV AWS_REGION=us-east-1

# Wrapper scripts use environment variables if provided
RUN printf "%s\n" \
  "#!/bin/bash" \
  "CONF_ARGS=\"\"" \
  "[ ! -z \"\$NESSIE_ENDPOINT\" ] && CONF_ARGS=\"\$CONF_ARGS --conf spark.sql.catalog.nessie.uri=\$NESSIE_ENDPOINT\"" \
  "[ ! -z \"\$MINIO_ENDPOINT\" ] && CONF_ARGS=\"\$CONF_ARGS --conf spark.hadoop.fs.s3a.endpoint=\$MINIO_ENDPOINT --conf spark.sql.catalog.nessie.s3.endpoint=\$MINIO_ENDPOINT\"" \
  "CONF_ARGS=\"\$CONF_ARGS --conf spark.sql.catalog.nessie.s3.path-style-access=true --conf spark.sql.catalog.nessie.s3.access-key-id=admin --conf spark.sql.catalog.nessie.s3.secret-access-key=password\"" \
  "exec spark-shell \$CONF_ARGS \"\$@\"" \
  > /usr/local/bin/spark-iceberg-nessie-shell && \
  printf "%s\n" \
  "#!/bin/bash" \
  "CONF_ARGS=\"\"" \
  "[ ! -z \"\$NESSIE_ENDPOINT\" ] && CONF_ARGS=\"\$CONF_ARGS --conf spark.sql.catalog.nessie.uri=\$NESSIE_ENDPOINT\"" \
  "[ ! -z \"\$MINIO_ENDPOINT\" ] && CONF_ARGS=\"\$CONF_ARGS --conf spark.hadoop.fs.s3a.endpoint=\$MINIO_ENDPOINT --conf spark.sql.catalog.nessie.s3.endpoint=\$MINIO_ENDPOINT\"" \
  "CONF_ARGS=\"\$CONF_ARGS --conf spark.sql.catalog.nessie.s3.path-style-access=true --conf spark.sql.catalog.nessie.s3.access-key-id=admin --conf spark.sql.catalog.nessie.s3.secret-access-key=password\"" \
  "exec spark-sql \$CONF_ARGS \"\$@\"" \
  > /usr/local/bin/spark-sql-iceberg-nessie-shell && \
  printf "%s\n" \
  "#!/bin/bash" \
  "CONF_ARGS=\"\"" \
  "[ ! -z \"\$NESSIE_ENDPOINT\" ] && CONF_ARGS=\"\$CONF_ARGS --conf spark.sql.catalog.nessie.uri=\$NESSIE_ENDPOINT\"" \
  "[ ! -z \"\$MINIO_ENDPOINT\" ] && CONF_ARGS=\"\$CONF_ARGS --conf spark.hadoop.fs.s3a.endpoint=\$MINIO_ENDPOINT --conf spark.sql.catalog.nessie.s3.endpoint=\$MINIO_ENDPOINT\"" \
  "CONF_ARGS=\"\$CONF_ARGS --conf spark.sql.catalog.nessie.s3.path-style-access=true --conf spark.sql.catalog.nessie.s3.access-key-id=admin --conf spark.sql.catalog.nessie.s3.secret-access-key=password\"" \
  "if [ \$# -gt 0 ]; then" \
  "  exec spark-submit \$CONF_ARGS \"\$@\"" \
  "else" \
  "  exec pyspark \$CONF_ARGS \"\$@\"" \
  "fi" \
  > /usr/local/bin/pyspark-iceberg-nessie-shell && \
  chmod +x /usr/local/bin/spark-iceberg-nessie-shell /usr/local/bin/spark-sql-iceberg-nessie-shell /usr/local/bin/pyspark-iceberg-nessie-shell

WORKDIR /opt/spark
CMD ["sleep", "infinity"]
