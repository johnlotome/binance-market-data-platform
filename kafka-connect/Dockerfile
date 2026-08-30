FROM debezium/connect:2.6

USER root

RUN mkdir -p /kafka/etc

# Download JMX Prometheus Java Agent
ENV JMX_AGENT_VERSION=0.20.0
RUN curl -sLS https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar \
    -o /kafka/etc/jmx_prometheus_javaagent.jar

COPY jmx-exporter-config.yml /kafka/etc/jmx-exporter-config.yml

USER kafka

ENV KAFKA_OPTS="-javaagent:/kafka/etc/jmx_prometheus_javaagent.jar=9404:/kafka/etc/jmx-exporter-config.yml"