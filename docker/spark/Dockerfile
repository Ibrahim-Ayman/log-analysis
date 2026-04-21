FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN HADOOP_VERSION=$(ls /opt/spark/jars/hadoop-client-api-*.jar \
    | sed 's/.*hadoop-client-api-\([0-9.]*\)\.jar/\1/' | head -n 1) && \
    if [ -z "$HADOOP_VERSION" ]; then HADOOP_VERSION="3.3.4"; fi && \
    echo "Detected Hadoop version: $HADOOP_VERSION" && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    -o "/opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    -o "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark