# Consistent Hashing with Redis

In Lab 1, you experienced the disaster of modulo hashing **firsthandâ€”75%** of your keys had to move when you scaled from 3 to 4 Redis nodes. That's unacceptable in production. In this lab, you'll implement consistent hashing with actual Redis instances and watch that number drop to ~25%.

## The Problem We're Solving

Recall from Lab 1: when using modulo hashing, the formula `hash(key) % node_count` creates a tight coupling between the hash result and the number of Redis nodes. Change the node count, and almost everything breaks.

**What we need:**
- A hash function that's **independent** of the number of nodes
- Keys that stay on the same Redis node when topology changes
- Minimal data movement during scale-up or scale-down events

## Consistent Hashing

Consistent hashing is a technique used in distributed systems to minimize data movement when database instances (or nodes) are added or removed. Traditional approachesâ€”such as hashing an ID and taking a modulo over the number of databasesâ€”cause *almost all data* to be reassigned whenever the number of databases changes. Consistent hashing avoids this problem by carefully structuring how data is mapped to databases.

**The solution:**  The core idea is to place **both data and database instances into the same circular hash space ranging from `0` to `2Â³Â² âˆ’ 1`**, commonly referred to as a **hash ring**.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel27.drawio.svg)

### The Hash Ring

To make the idea concrete, imagine a circular ring that represents a range of hash values. For simplicity, assume the hash space goes from **0 to 99**.

1. **Create the ring**
   The ring represents all possible hash values. Conceptually, the end wraps back to the beginning, so 99 is followed by 0.

2. **Place databases on the ring**
   Each database instance is assigned a position on the ring based on its own hash (for example, hashing its name or IP address).
   With four databases, we might place them evenly:

   * db0 at position 0
   * db1 at position 25
   * db2 at position 50
   * db3 at position 75

   ![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel22.drawio.svg)

3. **Map data to a database**
   To determine where a piece of data (such as an event) should live:

   * Hash the eventâ€™s ID to produce a value on the ring.
   * Starting from that position, **move clockwise** around the ring.
   * The first database instance you encounter is responsible for that data.

   For example, if `hash(event_id) = 10`, we locate position 10 on the ring and move clockwise until we hit `db1` at position 25. That event is stored on `db1`.

   ![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel23.drawio.svg)

This â€œmove clockwise to the next databaseâ€ rule is what gives consistent hashing its desirable properties.

### Why This Works

Because each database is responsible only for the section of the ring between itself and the previous database, changes to the system affect **only a small, well-defined portion of the data**.

## Adding a Database

Suppose we add a fifth database, `db4`, at position 85 on the ring.

* Before `db4` existed, the range from **75 to 99** mapped to `db0` (because after 75, the ring wraps around to 0).
* After adding DB5:

  * Only events whose hashes fall between **75 and 90** will now map to `db4`.
  * Events outside that range are completely unaffected.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel24.drawio.svg)

This means:

* Only a subset of the data previously handled by `db0` needs to move.
* All other databases keep their data exactly as before.

Instead of redistributing nearly all data, we only move a small fractionâ€”roughly proportional to the size of the new databaseâ€™s slice of the ring.

## Removing a Database

Now consider the opposite case: Database 2 (`db1` at position 25) fails or is removed.

* Only events that previously mapped to `db1` are affected.
* Those events will now map to the **next database clockwise**, which is `db2` at position 50.
* All other events remain on their original databases.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel26.drawio.svg)

Again, the impact is limited and predictable: data movement is confined to the portion of the ring owned by the removed database.

## Architecture Overview

Our new architecture looks similar to Lab 1, but with a crucial difference in the coordinator layer:

- **1. Client Layer:** Sends requests to the system

- **2. API Server Layer:** Routes requests (unchanged from Lab 1)

- **3. Consistent Hash Ring Coordinator:**  It creates a hash ring from `0` to `2Â³Â² âˆ’ 1`, places Redis nodes on the ring using `hash(node_name)`, maps each key to the first Redis node found clockwise from `hash(key)` using binary search for `O(log n)` lookups, and then connects to the corresponding Redis instance.


- **4. Redis Nodes:** Store data in actual Redis servers running in Docker

![Architecture](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel29.drawio.svg)

## Step by step Implementation

### Step 1: Initial Setup

Clone this repository and switch to the `lab/02` branch:

```bash
git clone https://github.com/poridhioss/Distributed_RedisCache.git
cd Distributed_RedisCache
git checkout lab/02
```

### Step 2: Codebase exploration

You now have the complete codebase for this project; before running it, letâ€™s examine how the consistent hashing algorithm is implemented in detail.

#### **ConsistentHashRing.js**

This is the heart of the solution. Create `ConsistentHashRing.js` in the coordinator directory:

**1. Hash Ring & Hash Space**

```js
this.hashSpace = Math.pow(2, 32); // 0 â†’ 2^32 - 1
```

Consistent hashing models the key space as a **circular ring**.

* The ring ranges from **0 to 2Â³Â²âˆ’1**
* Both **nodes** and **keys** are mapped to positions on this ring

**2. Hash Function (Keys & Nodes)**

A **deterministic hash function** maps:

* Node identifiers â†’ ring positions
* Data keys â†’ ring positions

Both **nodes and keys share the same hash function**, which is essential for consistent hashing.

```js
hashFunction(key) {
  const hash = crypto.createHash('sha256');
  hash.update(key);
  const hashHex = hash.digest('hex');
  const hashInt = parseInt(hashHex.substring(0, 8), 16);
  return hashInt % this.hashSpace;
}
```

**3. Adding Nodes to the Ring**

Each node is placed at **one position on the ring**. Each Redis node:

* Has a logical name (`cache_node_X`)
* Is hashed to a ring position

```js
const position = this.hashFunction(nodeName);
this.ring.set(position, nodeName);
this.sortedKeys.push(position);
this.sortedKeys.sort((a, b) => a - b);
```

**4. Ring Ordering (Clockwise Traversal)**

Consistent hashing requires finding the **next node clockwise**. `sortedKeys` keeps node positions in ascending order which enables efficient lookup using **binary search**

```js
this.sortedKeys.sort((a, b) => a - b);
```

**5. Mapping a Key to a Node**

To find the owner of a key:

1. Hash the key
2. Move clockwise on the ring
3. Pick the **first node â‰¥ key hash**
4. If none found â†’ wrap to the first node

```js
const keyPosition = this.hashFunction(key);
```

We have implemented the searching using Binary search Algorithm as it provides efficient **O(log N)** lookup:

```js
let left = 0;
let right = sortedKeys.length;

while (left < right) {
  const mid = Math.floor((left + right) / 2);
  if (this.sortedKeys[mid] < keyPosition) {
    left = mid + 1;
  } else {
    right = mid;
  }
}

// Wrap-Around Check
if (left >= this.sortedKeys.length) {
  left = 0;
}
```

Letâ€™s walk through an Example:

We have:

- Hash Space: [0, 99)
- Node positions (sortedKeys): [0, 25, 50, 75]

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel212.drawio.svg)

**Binary search Iteration Table:**

| Iteration | left | right | mid | sortedKeys[mid] | Comparison | Action    |
| --------- | ---- | ----- | --- | --------------- | ---------- | --------- |
| 1         | 0    | 4     | 2   | 50              | 50 < 30 âŒ  | right = 2 |
| 2         | 0    | 2     | 1   | 25              | 25 < 30 âœ…  | left = 2  |
| End       | 2    | 2     | â€”   | â€”               | â€”          | stop      |

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/Distributed_cachel211.drawio.svg)

You can also explore the other code files, which are largely the same as the Lab 01 version, with only minor changes to imports.

### Step 3: Start the Services

Now Start all services using Docker Compose:

```bash
docker compose up -d --build
```

Check if all services are running:

```bash
docker compose ps
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image.png)

View logs:

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f coordinator
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-1.png)

You should see the coordinator connecting to Redis nodes and the hash ring being initialized!

## Testing the API Endpoints

### Basic Cache Operations

**1. Add Endpoint:** Add Products to the Cache

```bash
curl -X POST http://localhost:4000/cache \
  -H "Content-Type: application/json" \
  -d '{
    "key": "product_1",
    "value": {"name": "Laptop", "price": 999}
  }' | jq
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-2.png)

Response shows which Redis node stored the key based on consistent hashing!

**2. Ring Visualization:** Visualize the Ring

This is new! Let's see where our Redis nodes are positioned on the ring:

```bash
curl http://localhost:4000/ring | jq
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-3.png)

Response shows:
- Each Redis node's position on the ring
- The range each node is responsible for
- The percentage of the hash space each node covers

### Load 1000 Products

Use the `populate-cache.sh` to load 1000 products into the cache. Before executing the script file first provide permission:

```bash
sudo chmod +x populate-cache.sh
```

Run the script:

```bash
./populate-cache.sh
```

Check distribution:

```bash
curl http://localhost:4000/distribution | jq
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-5.png)

**Note:** The distribution might be uneven. This is expected with only 3 Redis nodes. We'll solve this in Lab 3 with virtual nodes.

## The Moment of Truth: Scaling Up

Now let's see if consistent hashing actually solves the problem with actual Redis instances.

### Scale from 3 to 4 Redis Nodes

**Step 1:** Save current mappings

```bash
curl http://localhost:4000/mappings | jq > mappings-consistent-3nodes.json
```

**Step 2:** Stop the services and add the 4th Redis node

```bash
docker compose down -v
```

Edit `docker-compose.yml` and uncomment the `redis-node-3` section, update the coordinator's environment variable and uncomment the volume:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-15.png)

**Step 3:** Restart the services

```bash
docker compose up -d --build
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-8.png)

Now you have 4 Redis nodes with consistent hashing!

**Step 4:** Visualize the new ring

```bash
curl http://localhost:4000/ring | jq
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-9.png)

**See the difference?**
- `cache_node_3` was added at a specific position
- Only keys in one segment of the ring will move!

**Step 5:** Reload the 1000 products

```bash
./populate-cache.sh
```

**Step 6:** Get new mappings and compare

```bash
curl http://localhost:4000/mappings | jq > mappings-consistent-4nodes.json
```

Compare using the same script:

```bash
node compare-mappings.js
```

> **Note:** Update your `compare-mappings.js` to reference: `mappings-consistent-3nodes.json` and `mappings-consistent-4nodes.json`

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-10.png)

### **Consistent Hashing Results:**

- Keys stayed more than 75%

**Compare to Lab 1 (Modulo Hashing):**

- Keys that stayed: ~ 24.5%
- Keys that moved: ~ 75.5%

**That's a 3x improvement!**

We've reduced data movement by **66%** across actual Redis instances!

### Node Failure with Consistent Hashing

Let's see how consistent hashing handles a Redis node failure.

**Step 1:** Start with 4 Redis nodes, load 1000 products

> Ensure cache server have 4 nodes running

Save mappings:

```bash
curl http://localhost:4000/mappings | jq > mappings-consistent-4nodes-before.json
```

**Step 2:** Simulate Redis node 3 failure

```bash
docker compose down -v
```

Edit `docker-compose.yml` to comment out `redis-node-3` and update coordinator environment.

Restart:

```bash
docker compose up -d --build
```

**Step 3:** Reload products and compare

```bash
./populate-cache.sh
```
Save the mappings:

```bash
curl http://localhost:4000/mappings | jq > mappings-consistent-3nodes-after.json
```

Update file names:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-12.png)

```bash
node compare-mappings.js
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-13.png)

**Results:**
- Keys stayed more than 75%

**Perfect!** Only keys from the failed Redis node moved to the next node on the ring.

## Performance Analysis

### Time Complexity Comparison

| Operation | Modulo Hashing | Consistent Hashing |
|-----------|----------------|-------------------|
| Find Redis node for key | O(1) | O(log n) |
| Add Redis node | O(1) | O(log n) + data movement |
| Remove Redis node | O(1) | O(log n) + data movement |

**Trade-off:** 
- Slightly slower lookups (O(log n) vs O(1))
- But 3x less data movement during scaling
- The trade-off is worth it for real Redis deployments!

## The Remaining Problem: Uneven Distribution

You might notice that keys aren't perfectly distributed across Redis nodes:

```bash
curl http://localhost:4000/distribution | jq
```

You might see:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2002/images/image-14.png)

**Why?**

With only 3 Redis nodes, each node gets one position on the ring. The segments between nodes aren't equal, so some Redis instances handle more keys.

**The solution:** Virtual nodes (coming in Lab 3!)

Instead of placing each Redis node once on the ring, we'll place each node multiple times. This smooths out the distribution dramatically across all Redis instances.

## Conclusion

In this lab, you implemented consistent hashing with real Redis instances, showing that placing nodes on a fixed hash ring and assigning keys clockwise limits rebalancing to a single segmentâ€”keeping about 75% of keys in place during scalingâ€”at the cost of `O(log n)` lookups, making it well suited for production systems.