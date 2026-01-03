#!/usr/bin/env bash
set -euo pipefail

POM=pom.xml

# Very simple XML scraping; in a real setup use mvn help:evaluate or xmllint
spark_version=$(xmllint --xpath "string(//properties/spark.version)" "$POM")
scala_binary=$(xmllint --xpath "string(//properties/scala.binary)" "$POM")
iceberg_version=$(xmllint --xpath "string(//properties/iceberg.version)" "$POM")
nessie_version=$(xmllint --xpath "string(//properties/nessie.version)" "$POM")

echo "Using Spark $spark_version, Scala $scala_binary, Iceberg $iceberg_version, Nessie $nessie_version"

docker build \
  --build-arg SPARK_VERSION="$spark_version" \
  --build-arg SCALA_BINARY="$scala_binary" \
  --build-arg ICEBERG_VERSION="$iceberg_version" \
  --build-arg NESSIE_VERSION="$nessie_version" \
  -t rnalex/spark-iceberg-nessie:latest \
  -f Dockerfile .

