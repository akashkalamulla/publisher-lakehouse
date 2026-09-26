FROM python:3.12-slim-bookworm@sha256:392307d22300de8b5986851a12d9176dfc0fc073e65bf6523ebd7dcbeb23564e

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYTHONDONTWRITEBYTECODE=1

# The Python pins choose the Spark and Delta versions. Their JVM artifacts,
# including hadoop-aws's declared AWS SDK bundle dependency, are resolved here.
# Nothing invokes Maven when a job starts.
RUN set -eu; \
    apt-get update; \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless maven; \
    pip install --no-cache-dir pyspark==4.1.0 delta-spark==4.4.0 pytest==8.3.5; \
    python -c 'from pyspark.sql import SparkSession; s = SparkSession.builder.master("local[1]").appName("detect-hadoop").getOrCreate(); open("/tmp/hadoop-version", "w", encoding="utf-8").write(s._jvm.org.apache.hadoop.util.VersionInfo.getVersion()); s.stop()'; \
    HADOOP_VERSION="$(cat /tmp/hadoop-version)"; \
    test -n "$HADOOP_VERSION"; \
    echo "Bundled Hadoop version: $HADOOP_VERSION"; \
    printf '%s\n' \
      '<project xmlns="http://maven.apache.org/POM/4.0.0">' \
      '<modelVersion>4.0.0</modelVersion>' \
      '<groupId>local.lakehouse</groupId><artifactId>runtime-jars</artifactId><version>1</version>' \
      '<dependencies>' \
      '<dependency><groupId>io.delta</groupId><artifactId>delta-spark_4.1_2.13</artifactId><version>4.4.0</version></dependency>' \
      "<dependency><groupId>org.apache.hadoop</groupId><artifactId>hadoop-aws</artifactId><version>$HADOOP_VERSION</version></dependency>" \
      '</dependencies></project>' > /tmp/pom.xml; \
    mvn -B -q -f /tmp/pom.xml dependency:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory=/tmp/lake-jars; \
    test -f "/tmp/lake-jars/hadoop-aws-$HADOOP_VERSION.jar"; \
    test -f /tmp/lake-jars/delta-spark_4.1_2.13-4.4.0.jar; \
    cp /tmp/lake-jars/*.jar /usr/local/lib/python3.12/site-packages/pyspark/jars/; \
    rm -rf /tmp/lake-jars /tmp/pom.xml /tmp/hadoop-version /root/.m2; \
    apt-get purge -y --auto-remove maven; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    ! command -v mvn; \
    useradd --uid 10001 --create-home spark

WORKDIR /opt/app
USER spark
