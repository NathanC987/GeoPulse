# Spark 3.5 (Scala 2.12, Java 17, Python 3) with Kafka + Postgres connectors and the
# Python libraries the GeoPulse batch/streaming jobs need.
FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

ARG UID=1000
ARG GID=1000
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Connector jars: Structured Streaming <-> Kafka, and JDBC for Postgres (serving DB + Hive metastore).
RUN cd /opt/spark/jars && \
    for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar ; do \
        wget -q "$MAVEN/$jar" || exit 1; \
    done

RUN pip install --no-cache-dir \
      numpy==1.26.4 pandas==2.2.3 pyarrow==17.0.0 scipy==1.13.1 \
      h3==4.1.2 psycopg2-binary==2.9.10

# Run as the host user's uid so files written to the bind-mounted ./data stay editable on the host.
RUN groupmod -g ${GID} spark && usermod -u ${UID} -g ${GID} spark && \
    mkdir -p /opt/geopulse /home/spark && chown -R spark:spark /opt/spark/work-dir /opt/geopulse /home/spark

COPY spark-defaults.conf hive-site.xml log4j2.properties /opt/spark/conf/
COPY submit.sh /usr/local/bin/submit
RUN chmod +x /usr/local/bin/submit

ENV PYTHONPATH=/opt/geopulse/app \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH
WORKDIR /opt/geopulse/app
USER spark
