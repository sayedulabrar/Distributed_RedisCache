#!/bin/bash

echo "Generating rapid writes to test replication lag..."

# Write 1000 keys as fast as possible
for i in {1..1000}; do
  curl -s -X POST http://localhost/cache \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"load:$i\",\"value\":\"data$i\"}" > /dev/null &
done

# Wait a moment for writes to start
sleep 0.5

# Check lag immediately
echo "Checking replication lag during load..."
curl -s http://localhost/replication/lag | jq

# Wait for all writes to complete
wait

# Check lag after load
echo ""
echo "Waiting 2 seconds for replication to catch up..."
sleep 2

echo "Checking replication lag after load..."
curl -s http://localhost/replication/lag | jq