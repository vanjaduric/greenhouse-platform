#!/bin/bash
KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP=kafka:9092

for topic in sensors.temperature sensors.humidity sensors.co2 sensors.light sensors.airflow; do
    $KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
        --topic $topic \
        --bootstrap-server $BOOTSTRAP \
        --partitions 1 \
        --replication-factor 1
done

echo "Kafka topics ready"