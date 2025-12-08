#!/bin/bash
# Rebuild and restart all services

cd "$(dirname "$0")"
docker compose -f infrastructure/docker/docker-compose.yml down
docker compose -f infrastructure/docker/docker-compose.yml build
docker compose -f infrastructure/docker/docker-compose.yml up -d
echo "Waiting for services to start..."
sleep 3
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
