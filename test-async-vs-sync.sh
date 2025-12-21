#!/bin/bash

echo "=== Testing ASYNC Replication (Default) ==="
echo "Writing 100 keys with async replication..."

start_time=$(date +%s%N)

for i in {1..100}; do
  curl -s -X POST http://localhost/cache \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"async:$i\",\"value\":\"data$i\"}" > /dev/null
done

end_time=$(date +%s%N)
async_time=$(( (end_time - start_time) / 1000000 ))

echo "Async replication: $async_time ms for 100 writes"
echo "Average per write: $((async_time / 100)) ms"

echo ""
echo "=== Testing SYNC Replication ==="
echo "Writing 100 keys with sync replication..."

start_time=$(date +%s%N)

for i in {1..100}; do
  curl -s -X POST http://localhost/cache \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"sync:$i\",\"value\":\"data$i\",\"replicationMode\":\"sync\"}" > /dev/null
done

end_time=$(date +%s%N)
sync_time=$(( (end_time - start_time) / 1000000 ))

echo "Sync replication: $sync_time ms for 100 writes"
echo "Average per write: $((sync_time / 100)) ms"

echo ""
echo "=== Performance Comparison ==="
echo "Async: $async_time ms"
echo "Sync:  $sync_time ms"
echo "Sync is $(echo "scale=1; $sync_time / $async_time" | bc)x slower"