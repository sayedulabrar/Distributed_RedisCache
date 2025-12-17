#!/bin/bash

for i in {1..1000}; do
  curl -X POST http://localhost:4000/cache \
    -H "Content-Type: application/json" \
    -d "{\"key\": \"product_$i\", \"value\": {\"id\": $i, \"name\": \"Product $i\", \"price\": $((RANDOM % 1000))}}" \
    -s > /dev/null
  
  if [ $((i % 100)) -eq 0 ]; then
    echo "Inserted $i products..."
  fi
done

echo "Done! Inserted 1000 products."