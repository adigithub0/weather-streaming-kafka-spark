#!/bin/bash

# Replace with your actual UUID
KAFKA_UUID="aZXnilqARReBQrEP0Hwc0Q"

echo "[1/4] Stopping existing Kafka processes..."
pkill -f kafka || true

echo "[2/4] Cleaning data folder..."
rm -rf ~/kafka/data
mkdir -p ~/kafka/data

echo "[3/4] Formatting storage..."
~/kafka/bin/kafka-storage.sh format -t $KAFKA_UUID -c ~/kafka/config/kraft/server.properties

echo "[4/4] Starting Kafka..."
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties