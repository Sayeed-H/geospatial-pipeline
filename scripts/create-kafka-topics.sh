#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# Create all required Kafka topics
# Run after docker-compose is up
# ─────────────────────────────────────────────────────────────────

KAFKA_BROKER=${KAFKA_BROKER:-localhost:9092}

echo "⏳ Waiting for Kafka to be ready..."
sleep 10

create_topic() {
  local topic=$1
  local partitions=$2
  local retention_ms=$3

  echo "📌 Creating topic: $topic (partitions: $partitions)"
  docker exec kafka kafka-topics \
    --bootstrap-server $KAFKA_BROKER \
    --create \
    --if-not-exists \
    --topic $topic \
    --partitions $partitions \
    --replication-factor 1 \
    --config retention.ms=$retention_ms
}

# raw GPS - high partition count for parallelism
create_topic "raw.gps.events"       8  604800000   # 7 days

# enriched GPS (after Flink H3 tagging)
create_topic "enriched.gps.events"  8  604800000

# trip lifecycle events
create_topic "trip.lifecycle.events" 4 2592000000  # 30 days

# anomaly events
create_topic "anomaly.events"       4  2592000000

# zone aggregations output
create_topic "zone.aggregations"    4  86400000    # 1 day

echo ""
echo "✅ All Kafka topics created!"
echo ""
docker exec kafka kafka-topics --bootstrap-server $KAFKA_BROKER --list