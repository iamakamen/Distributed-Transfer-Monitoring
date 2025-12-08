#!/bin/bash
# Start all services using docker compose

cd "$(dirname "$0")"
docker compose -f infrastructure/docker/docker-compose.yml up -d "$@"
