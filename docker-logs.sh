#!/bin/bash
# View logs for a specific service
# Usage: ./docker-logs.sh [service-name]
# Example: ./docker-logs.sh dtms-producer

if [ -z "$1" ]; then
  echo "Usage: ./docker-logs.sh <container-name>"
  echo "Available containers: dtms-producer, dtms-spark-exporter, dtms-exporter, dtms-anomaly-exporter"
  exit 1
fi

docker logs "$1" "${@:2}"
