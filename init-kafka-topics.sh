#!/bin/bash
echo "Waiting for Kafka to be ready..."
sleep 15

KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP=kafka:9092

echo "Creating topics..."

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists --topic sensors.temperature --bootstrap-server $BOOTSTRAP --partitions 3 --replication-factor 1
echo "sensors.temperature ready"

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists --topic sensors.humidity --bootstrap-server $BOOTSTRAP --partitions 3 --replication-factor 1
echo "sensors.humidity ready"

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists --topic sensors.co2 --bootstrap-server $BOOTSTRAP --partitions 3 --replication-factor 1
echo "sensors.co2 ready"

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists --topic sensors.light --bootstrap-server $BOOTSTRAP --partitions 3 --replication-factor 1
echo "sensors.light ready"

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists --topic sensors.airflow --bootstrap-server $BOOTSTRAP --partitions 3 --replication-factor 1
echo "sensors.airflow ready"

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists --topic events.irrigation --bootstrap-server $BOOTSTRAP --partitions 3 --replication-factor 1
echo "events.irrigation ready"

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists --topic dead.letter.queue --bootstrap-server $BOOTSTRAP --partitions 1 --replication-factor 1
echo "dead.letter.queue ready"

echo "All topics created successfully"