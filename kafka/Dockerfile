FROM apache/kafka:4.0.0

ARG KAFKA_VERSION=4.0.0
ARG SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka

WORKDIR /opt

USER root
RUN apk add --no-cache curl
RUN mkdir -p /opt/jmx_exporter && \
    wget -O /opt/jmx_exporter/jmx_prometheus_javaagent.jar \
      https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar && \
    chown -R appuser:appuser /opt/jmx_exporter
USER appuser

COPY --chmod=755 ./start-kafka.sh /opt/kafka/bin/start-kafka.sh

ENV PATH="$KAFKA_HOME/bin:$PATH"