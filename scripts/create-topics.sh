#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T broker kafka-topics \
  --bootstrap-server broker:29092 \
  --create \
  --if-not-exists \
  --topic product.events \
  --partitions 3 \
  --replication-factor 1

docker compose exec -T broker kafka-topics \
  --bootstrap-server broker:29092 \
  --create \
  --if-not-exists \
  --topic product.events.dlq \
  --partitions 1 \
  --replication-factor 1

docker compose exec -T broker kafka-topics \
  --bootstrap-server broker:29092 \
  --create \
  --if-not-exists \
  --topic product.events.dlq.indexer.dlq \
  --partitions 1 \
  --replication-factor 1
