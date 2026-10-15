#!/bin/sh
# Starts a disposable three-node Redis Cluster (no replicas) in one
# container, listening on 127.0.0.1:7000-7002, for integration tests.
# Usage: scripts/redis-cluster-up.sh [image]   (default redis:7.4-alpine)
set -eu
image="${1:-redis:7.4-alpine}"
name=rategen-cluster
if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
	docker rm -f "$name" >/dev/null 2>&1 || true
	docker run -d --name "$name" -p 7000-7002:7000-7002 "$image" sh -c '
		for p in 7000 7001 7002; do
			redis-server --port $p --cluster-enabled yes --cluster-config-file nodes-$p.conf \
				--cluster-node-timeout 5000 --cluster-announce-ip 127.0.0.1 \
				--appendonly no --save "" --daemonize yes
		done
		sleep 1
		redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 --cluster-replicas 0 --cluster-yes
		tail -f /dev/null' >/dev/null
fi
until docker exec "$name" redis-cli -p 7000 cluster info 2>/dev/null | grep -q 'cluster_state:ok'; do sleep 0.5; done
echo "export RATEGEN_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002"
