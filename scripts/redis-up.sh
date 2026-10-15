#!/bin/sh
# Starts a disposable Redis for integration tests and prints the variable to
# export. Usage: scripts/redis-up.sh [image]   (default redis:7.4-alpine)
set -eu
image="${1:-redis:7.4-alpine}"
name=rategen-redis
if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
	docker rm -f "$name" >/dev/null 2>&1 || true
	docker run -d --name "$name" -p 6379:6379 "$image" >/dev/null
fi
until docker exec "$name" redis-cli ping >/dev/null 2>&1; do sleep 0.2; done
echo "export RATEGEN_REDIS_ADDR=127.0.0.1:6379"
