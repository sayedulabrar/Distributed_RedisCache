# Distributed Cache API with Load Balancing

In the previous lab, We have built a distributed cache with perfect load distribution. But that's only half the story. In real production systems, we need multiple API servers, load balancing, comprehensive metrics, and the ability to handle thousands of requests per second.

In this lab, we'll build a **distributed cache service** with multiple API servers, load balancing, metrics collection, and real performance testing.

## Architecture Overview

In this lab, we extend the existing system to support scalability, high availability, and better performance under production-like workloads. The architecture is enhanced by introducing:

- Multiple `API servers` behind an `Nginx` load balancer to eliminate single points of failure and evenly distribute incoming traffic. 

- A centralized coordinator using virtual nodes (VNodes) is used to efficiently route requests to the appropriate Redis instances, improving data distribution and balancing load across the cache layer. 

Additionally, the system incorporates connection pooling and metrics collection to optimize resource usage and provide real-time visibility into performance, while load testing is used to validate the systemâ€™s behavior under increased traffic.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/Distributed_cachel401.drawio.svg)

## Environment Setup

Clone the repository and switch to the fifth lab:

```bash
git clone https://github.com/poridhioss/Distributed_RedisCache.git
git checkout lab/04
cd Distributed_RedisCache
```
The codebase has the following structure:

```bash
distributed-cache-api/
â”œâ”€â”€ api-server/
â”‚   â”œâ”€â”€ package.json
â”‚   â”œâ”€â”€ Dockerfile
â”‚   â””â”€â”€ server.js
â”œâ”€â”€ coordinator/
â”‚   â”œâ”€â”€ package.json
â”‚   â”œâ”€â”€ Dockerfile
â”‚   â”œâ”€â”€ coordinator.js
â”‚   â””â”€â”€ ConsistentHashRingWithVNodes.js
â”œâ”€â”€ nginx/
â”‚   â”œâ”€â”€ Dockerfile
â”‚   â””â”€â”€ nginx.conf
â”œâ”€â”€ docker-compose.yml
â””â”€â”€ load-test/
    â”œâ”€â”€ package.json
    â””â”€â”€ load-test.js
```

## Codebase Exploration:

### Nginx Load Balancer Configuration

This NGINX instance is configured as a **Layer-7 (HTTP) reverse proxy and load balancer** in front of **three API servers**:

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/Distributed_cachel402.drawio.svg)

Its goals are to:

* Distribute incoming HTTP traffic evenly
* Improve performance (keep-alive, gzip, epoll)
* Provide fault tolerance (health/failure detection)
* Act as a single entry point for clients
* Expose observability (logs, metrics, status)

Lets explore the key configurations in the `nginx.conf` file: 

**1. Global NGINX process configuration**

```nginx
events {
    worker_connections 1024;
    use epoll;
}
```

* Each worker can handle **1024 concurrent connections**
* `epoll` is a **high-performance event model** (Linux-optimized)
* Total theoretical concurrency `workers* 1024`

This is optimized for **high-throughput, low-latency** workloads.

**2. Defination of the variables used**

---

### Client / Request Variables (always available)

| Variable                | Meaning                                      |
| ----------------------- | -------------------------------------------- |
| `$remote_addr`          | Client IP address                            |
| `$remote_user`          | Authenticated username (if using HTTP auth)  |
| `$time_local`           | Local server time when request was processed |
| `$request`              | Full request line (e.g. `GET /api HTTP/1.1`) |
| `$status`               | HTTP response status code                    |
| `$body_bytes_sent`      | Response size sent to client                 |
| `$http_referer`         | `Referer` header from the client             |
| `$http_user_agent`      | `User-Agent` header                          |
| `$http_x_forwarded_for` | `X-Forwarded-For` header from request        |

These work **for any request**, even if you're not proxying.

---

### Request Timing Variable

| Variable        | Meaning                                       |
| --------------- | --------------------------------------------- |
| `$request_time` | Total time NGINX spent processing the request |

This includes:

```
client request → nginx processing → upstream → response sent
```

---

### Upstream (Proxy) Variables

These **only have values if you're using `proxy_pass` / upstream servers**.

| Variable                  | Meaning                                    |
| ------------------------- | ------------------------------------------ |
| `$upstream_connect_time`  | Time to connect to backend server          |
| `$upstream_header_time`   | Time until upstream sends response headers |
| `$upstream_response_time` | Total time backend took to respond         |

If the request **doesn't hit an upstream**, these may appear as:

```
-
```

in logs.

---

### Example Log Entry

Example output:

```
192.168.1.10 - - [11/Mar/2026:12:40:12 +0000] "GET /api/users HTTP/1.1"
200 512 "-" "PostmanRuntime/7.36" "-"
rt=0.145 uct="0.002" uht="0.010" urt="0.140"
```

Meaning:

* Total request time → **145 ms**
* Backend connect → **2 ms**
* Backend header response → **10 ms**
* Backend full response → **140 ms**

---

---

# 1️⃣ The Minimal Config (what actually works)

You technically only need this to proxy to your backend:

```nginx
location / {
    proxy_pass http://api_backend;
}
```

NGINX will still work.

Everything else you showed is mostly about:

* preserving client information
* improving performance
* preventing stuck connections
* helping backend apps behave correctly

---

# 2️⃣ The 3 settings that matter the most

These are the **important ones in real systems**.

### Forward client IP

```nginx
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
```

Without this:

Your backend sees

```
client IP = nginx server
```

Instead of the **real user IP**.

Frameworks like:

* Express
* Django
* Laravel
* Spring

use this header to know the **real client IP**.

---

### Forward protocol

```nginx
proxy_set_header X-Forwarded-Proto $scheme;
```

Without this:

Backend might think request is **HTTP** even if user used **HTTPS**.

This breaks things like:

* secure cookies
* redirects
* OAuth callbacks

---

### Forward host

```nginx
proxy_set_header Host $host;
```

Without this:

Backend might see

```
Host: api_backend
```

instead of

```
Host: example.com
```

Some frameworks use Host for:

* routing
* generating links
* multi-tenant apps

---

# 3️⃣ Performance optimization

### HTTP version

```nginx
proxy_http_version 1.1;
```

Why?

HTTP/1.1 supports **connection reuse (keepalive)**.

Without it:

NGINX may open **a new TCP connection for every request** → slower.

---

### Connection header

```nginx
proxy_set_header Connection "";
```

This removes headers that might **break keepalive**.

Mostly for **clean proxy behavior**.

---

# 4️⃣ Safety timeouts

These protect your server from hanging connections.

### Connect timeout

```nginx
proxy_connect_timeout 5s;
```

If backend doesn't respond in **5 seconds**, stop trying.

Without it:

NGINX might wait **very long**.

---

### Send timeout

```nginx
proxy_send_timeout 60s;
```

Max time to send request to backend.

---

### Read timeout

```nginx
proxy_read_timeout 60s;
```

Max time to wait for backend response.

Without these:

A stuck backend could **hang NGINX workers**.

---

# 5️⃣ Real-world mental model

Think of NGINX as a **reception desk** in front of your API.

These settings tell the receptionist:

* "Tell the doctor who the real patient is" → `X-Forwarded-For`
* "Tell the doctor whether patient came via secure door" → `X-Forwarded-Proto`
* "Tell the doctor which clinic the patient asked for" → `Host`
* "Don't wait forever for a doctor" → timeouts
* "Reuse phone line instead of redialing" → HTTP/1.1

---


**3. Upstream backend definition (core of load balancing)**

---
* `api-server-1:4000` → hostname + port of backend API.
* `max_fails=3` → if **3 requests fail**, NGINX marks the server as failed.
* `fail_timeout=30s` → server is **temporarily disabled for 30 seconds**.
* This improves **fault tolerance**.

* `keepalive 32;` Allows **32 idle keepalive connections** to backend servers.
* NGINX can reuse these connections instead of opening new ones.
* Improves **performance and reduces latency**.

---

```nginx
upstream api_backend {
    least_conn;
    server api-server-1:4000 max_fails=3 fail_timeout=30s;
    server api-server-2:4000 max_fails=3 fail_timeout=30s;
    server api-server-3:4000 max_fails=3 fail_timeout=30s;
    keepalive 32;
}
```

The loadbalancing algorithm used here is `least_conn`. Requests go to the backend with the **fewest active connections**. Better than round-robin for:
  * Variable request durations
  * Mixed read/write workloads
  * Uneven backend performance

**4. Request routing (how traffic flows)**

**Default Route:**

```nginx
location / {
    proxy_pass http://api_backend;
}
```

Every incoming request:

1. Hits NGINX on port 80
2. Is forwarded to `api_backend`
3. NGINX chooses the backend using **least-connections**
4. Response is returned to the client

**Health checks**

```nginx
location /health {
    proxy_pass http://api_backend/health;
}
```

Proxies health checks to backend services

**Metrics**

```nginx
location /metrics {
    proxy_pass http://api_backend/metrics;
}
```

* Exposes backend metrics through NGINX
* Keeps monitoring endpoints private to internal networks

### API Server

This file `api-server/server.js` defines a horizontally scalable API server that sits behind NGINX, adds a fast local cache layer, forwards authoritative cache operations to a coordinator, and exposes rich metrics and health endpoints for a distributed caching system.


---

### L1 Cache (API server)

```
API Server
   └── Local Cache (Map, TTL 5s)
```

* Stored in memory inside each API node
* Very fast
* Lost if the server restarts
* Used only for **very short-term caching**

Example:

```js
const localCache = new Map();
```

This is often called **L1 cache**.

---

### L2 Cache (Coordinator / Redis)

```
API Server
   ↓
Coordinator
   ↓
Redis
```

Redis acts as:

* Distributed cache
* Shared across all API nodes
* Much larger than local cache
* Still **not permanent storage**

---

### Current storage stack

```
Client
  ↓
NGINX
  ↓
API servers
  ↓
Local Cache (L1)
  ↓
Coordinator
  ↓
Redis (L2)
```

**there is no database here.**


**1. Local in-memory cache (L1 cache)**

```bash
const localCache = new Map();
const LOCAL_CACHE_TTL = 5000;
const LOCAL_CACHE_MAX_SIZE = 1000;
```

This is a per-node, short-lived cache layer. Reduces calls to the coordinator / Redis. Improves latency for hot keys. Each server maintains its own cache.

**2. Axios coordinator client (connection pooling)**

```js
const coordinatorClient = axios.create({ ... });
```

Configured with:

* Base URL of coordinator
* 5s timeout
* **HTTP keep-alive**
* Connection pool (max 50 sockets)

Purpose:

* Efficient communication with coordinator
* Reduces TCP overhead under high load
* Prevents request storms from opening too many connections

**3. POST `/cache` â€” Set a value**

```js
app.post('/cache', async (req, res) => { ... });
```

Flow:

1. Validate request body
2. Forward request to coordinator
3. Update cache metrics (`sets`)
4. Store value in local cache
5. Return coordinator response


**4. GET `/cache/:key` â€” Read a value**

```js
app.get('/cache/:key', async (req, res) => { ... });
```

Read path:

1. **Check local cache first**

   * If hit â†’ return immediately (fast path)
2. If miss â†’ query coordinator
3. If found:

   * Store in local cache
   * Return value
4. If not found:

   * Return 404

Metrics updated:

* Cache hits / misses
* Latency tracking

This implements a **read-through cache strategy**.

**5. DELETE `/cache/:key` â€” Delete a value**

```js
app.delete('/cache/:key', async (req, res) => { ... });
```

Flow:

1. Remove key from local cache
2. Forward delete to coordinator
3. Update delete metrics
4. Return response

Purpose:

* Ensures local cache does not serve stale data
* Coordinator handles global invalidation

### Coordinator Service

The coordinator is identical to our previous codebase.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/Distributed_cachel403.drawio.svg)

## Testing the System

### Test 1: Basic Functionality

Start all services:

```bash
docker compose up -d --build
```

Check all services are healthy:

```bash
docker compose ps
```

You should see all services running and healthy:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image.png)

### Test 2: Verify Load Balancing

Make several requests and check which server handles them:

```bash
for i in {1..10}; do
  curl -s http://localhost/health | jq -r '.serverId'
done
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-1.png)

Expected output (servers should rotate):

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-2.png)

### Test 3: Test Cache Operations

Set a value:

```bash
curl -X POST http://localhost/cache \
  -H "Content-Type: application/json" \
  -d '{
    "key": "user_123",
    "value": {"name": "Alice", "age": 30},
    "ttl": 300
  }' | jq
```

Response:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-3.png)

Get the value:

```bash
curl http://localhost/cache/user_123 | jq
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-4.png)

### Test 4: Populate Cache

```bash
./populate-cache.sh
```

Check distribution:

```bash
curl http://localhost:3001/distribution | jq
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-5.png)

### Test 5: Check Metrics

Get metrics from a specific API server:

```bash
curl http://localhost/metrics | jq
```

Response:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-6.png)

**Key Metrics to Observe:**
- **Hit Rate** - Should improve over time as local cache warms up
- **P95/P99 Latency** - Should be consistently low
- **Requests/Second** - Throughput capacity
- **Local Cache Size** - Shows caching effectiveness

### Test 6: Nginx Status

Check nginx load balancer status:

```bash
curl http://localhost:8080/nginx_status
```

Response:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-7.png)

### Test 7: Run Load Test

```bash
cd load-test
npm install
node load-test.js
```

Or with custom parameters:

```bash
DURATION=30 CONCURRENCY=20 KEY_SPACE=5000 node load-test.js
```

Expected output:

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-8.png)

**What to Look For:**
- **Throughput**
- **Success Rate**
- **P95 Latency**
- **Server Distribution**
- **Errors**

## Performance Analysis

### Load Balancing Effectiveness

With 3 API servers, requests should distribute evenly:

```bash
# Check request distribution
for server in 1 2 3; do
  echo -n "Server $server: "
  docker exec api_server_$server wget -qO- http://localhost:4000/metrics | \
    jq -r '.requests.total'
done
```

Expected (roughly equal):

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-9.png)

### Local Cache Impact

The local cache on each API server significantly improves performance:

**Without local cache:**
- All requests â†’ Coordinator â†’ Redis
- Latency: ~15-20ms average

**With local cache:**
- Hot keys served from API server memory
- Latency: ~3-5ms for cached items
- Overall P50: ~10ms (significant improvement!)

Check local cache effectiveness:

```bash
curl http://localhost/metrics | jq '.cache'
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-10.png)

## Testing Failure Scenarios

### Scenario 1: Single API Server Failure

Stop one API server:

```bash
docker stop api_server_2
```

The system should continue operating:

```bash
# Requests should still work
curl http://localhost/cache/user_123 | jq

# Load should redistribute to remaining servers
for i in {1..10}; do
  curl -s http://localhost/health | jq -r '.serverId'
done
```

Output (only servers 1 and 3):

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/bd102bd056670398f0f1d112e260e17c14cc337d/Redis%20Distributed%20Cache/Lab%2004/images/image-11.png)

Restart the server:

```bash
docker start api_server_2
```

Nginx automatically re-includes it in the pool!

### Scenario 2: Multiple API Server Failures

Stop two API servers:

```bash
docker stop api_server_2 api_server_3
```

System still works with just one server:

```bash
curl http://localhost/health | jq
```

### Scenario 3: Load During Failure

Run load test while stopping a server:

```bash
# Terminal 1: Start load test
cd load-test
node load-test.js &

# Terminal 2: Stop a server after 10 seconds
sleep 10 && docker stop api_server_2
```

Observe:
- Small spike in error rate during failover
- System recovers automatically
- Throughput drops to ~66% temporarily
- No complete outage!

## Conclusion

In this lab, we have successfully designed and implemented a production-grade distributed cache system that demonstrates key real-world system design principles. By combining multiple API servers behind an NGINX load balancer, adding a local caching layer, enabling metrics and health checks, and validating the system with realistic load testing, you achieved a scalable, resilient, and high-performance architecture. This setup effectively handles traffic distribution, minimizes latency, and provides strong observability and fault tolerance, closely mirroring how modern distributed caching systems are built in practice.