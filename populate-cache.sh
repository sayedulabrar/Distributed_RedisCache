#!/bin/bash

echo "Populating cache with 1000 products..."

for i in {1..1000}; do
  curl -s -X POST http://localhost/cache \
    -H "Content-Type: application/json" \
    -d "{
      \"key\": \"product_$i\",
      \"value\": {
        \"id\": $i,
        \"name\": \"Product $i\",
        \"price\": $(($RANDOM % 1000 + 1)),
        \"category\": \"Category $((i % 10))\"
      },
      \"ttl\": 3600
    }" > /dev/null
  
  if [ $((i % 100)) -eq 0 ]; then
    echo "Inserted $i products..."
  fi
done

echo "Cache populated with 1000 products"