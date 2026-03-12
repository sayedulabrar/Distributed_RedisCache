# Express Middleware Response Interception (res.send Override)

## Purpose

Intercept the response in **Express.js** to:

* measure request latency
* log request/response info
* update metrics
* add custom headers
* still send the original response

---

# Basic Middleware Code Pattern

```js
app.use((req, res, next) => {
  const startTime = Date.now();
  
  // Log request
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} - Server ${SERVER_ID}`);
  
  // Capture response
  const originalSend = res.send;
  res.send = function(data) {
    const duration = Date.now() - startTime;
    
    // Store latency (keep last 10000)
    if (metrics.latencies.length >= 10000) {
      metrics.latencies.shift();
    }
    metrics.latencies.push(duration);
    
    // Update request metrics
    metrics.requests.total++;
    if (res.statusCode >= 200 && res.statusCode < 400) {
      metrics.requests.successful++;
    } else {
      metrics.requests.failed++;
    }
    
    // Add performance headers
    res.setHeader('X-Response-Time', `${duration}ms`);
    res.setHeader('X-Server-ID', SERVER_ID);
    
    originalSend.call(this, data);
  };
  
  next();
});
```

---

# Full Request–Response Flow

## 1. Request arrives

Client sends request → Express receives it.

Middleware runs first.

```
Request → Middleware
```

---

## 2. Middleware initializes request timer

```js
const startTime = Date.now();
```

This marks the **request start time**.

Each request gets its **own startTime variable**.

---

## 3. Original `res.send` is stored

```js
const originalSend = res.send;
```

Why?

Because we will **override `res.send`**, but still need to call the original function later.

---

## 4. `res.send` is overridden

```js
res.send = function(data) {
```

Now whenever the route calls:

```
res.send(...)
```

it will execute **our wrapper function first**.

---

## 5. Middleware passes control

```js
next();
```

Express continues to:

```
next middleware → route handler
```

---

## 6. Route handler executes

Example:

```js
app.get("/users", (req, res) => {
  res.send({ name: "Rahim" });
});
```

The API does its logic (DB calls, processing, etc.).

---

## 7. Route calls `res.send`

Because we **overrode `res.send` earlier**, the wrapper function runs.

```
Route → res.send() → our wrapper
```

---

## 8. Wrapper calculates latency

```js
const duration = Date.now() - startTime;
```

`startTime` comes from the middleware scope.

This works due to **Closure**.

It measures:

```
request arrival → response send
```

---

## 9. Custom logic runs

Example:

### Update metrics

```
metrics.requests.total++
metrics.latencies.push(duration)
```

### Add headers

```js
res.setHeader("X-Response-Time", "120ms")
res.setHeader("X-Server-ID", SERVER_ID)
```

---

## 10. Original Express response is sent

```js
originalSend.call(this, data);
```

This calls the **real Express `res.send()`**.

Response now goes to the client.

---

# Final HTTP Response Example

```
HTTP/1.1 200 OK
Content-Type: application/json
X-Response-Time: 120ms
X-Server-ID: server-2

{"name":"Rahim"}
```

The response contains:

* original body (JSON)
* new headers added by middleware

---

# Request Isolation

Each request has its own scope.

Example:

```
Request A → startTime_A
Request B → startTime_B
Request C → startTime_C
```

Each overridden `res.send` remembers its own `startTime`.

So requests **do not interfere with each other**.

---

# Lifecycle of `startTime`

```
Request arrives
    ↓
startTime created
    ↓
API executes
    ↓
res.send called
    ↓
duration calculated
    ↓
response sent
    ↓
request scope released (garbage collected)
```

Next request → **new startTime created**.

---

# Why this technique is used

Common uses in backend systems:

* request latency tracking
* metrics collection
* observability
* distributed tracing
* load balancer debugging
* logging for **NGINX** or monitoring systems

---

# Mental Model (Easy Way to Remember)

Think of it as:

```
Middleware (setup timer + intercept send)
        ↓
Original API executes
        ↓
Response intercepted
        ↓
Metrics + headers added
        ↓
Original response sent
```

---

## 📝 Short Note on LRU Cache Using Map in JS

A **Least Recently Used (LRU) cache** stores a limited number of items and evicts the **least recently accessed** item when full. In JavaScript:

* `Map` preserves **insertion order**, which can be leveraged for LRU.
* **Accessing a key** does **not** automatically move it to the end.
  To mark it as recently used, we **delete it and re-insert it**.
* **Eviction** removes the **first key** in the `Map`, which is the **least recently used**.
* Optionally, each entry can store a `timestamp` to enforce a TTL (time-to-live).

This approach gives **O(1)** get and set operations and a simple LRU mechanism without a linked list.

---

## 🔹 Rewritten LRU Cache Functions

```javascript
const LOCAL_CACHE_TTL = 5 * 60 * 1000; // 5 minutes TTL
const LOCAL_CACHE_MAX_SIZE = 100;
const localCache = new Map();

function getFromLocalCache(key) {
  const cached = localCache.get(key);
  if (!cached) return null;

  // Check TTL
  if (Date.now() - cached.timestamp > LOCAL_CACHE_TTL) {
    localCache.delete(key);
    return null;
  }

  // Move key to end to mark as recently used
  localCache.delete(key);
  localCache.set(key, cached);

  return cached.value;
}

function setToLocalCache(key, value) {
  // Evict least recently used if full
  if (localCache.size >= LOCAL_CACHE_MAX_SIZE) {
    const firstKey = localCache.keys().next().value;//description in next section
    localCache.delete(firstKey);
  }

  localCache.set(key, {
    value,
    timestamp: Date.now()
  });
}
```

✅ Key points in this version:

1. **Access moves the key to the end** (recently used) via `delete + set`.
2. **Eviction always removes the oldest key** (least recently used) using `Map.keys().next().value`.
3. Supports **TTL expiration** for cache entries.

---


## 1️⃣ `Map.keys()` returns an iterator

```javascript
const map = new Map();
map.set("A", 1);
map.set("B", 2);
map.set("C", 3);

const iter = map.keys();  // returns an iterator
```

At this point:

* `iter` doesn’t contain all keys as an array.
* It just **remembers the position** in the sequence of keys (like a cursor/pointer).

---

## 2️⃣ Using `.next()`

The iterator has a `.next()` method. Each call gives you an object:

```javascript
{ value: <current key>, done: <boolean> }
```

* `value` → the current key
* `done` → `false` if there are more keys, `true` if you’ve reached the end

Example:

```javascript
console.log(iter.next()); // { value: "A", done: false }
console.log(iter.next()); // { value: "B", done: false }
console.log(iter.next()); // { value: "C", done: false }
console.log(iter.next()); // { value: undefined, done: true }
```

✅ Notice that once `done` is `true`, there are no more keys.

---

## 3️⃣ Like a pointer

* You can think of the iterator as a **pointer moving through the keys in insertion order**.
* Each `.next()` moves the pointer forward.
* It **doesn’t reset automatically**, but you can create a new iterator with `map.keys()` if you want to iterate again.

---

## 4️⃣ Example: iterate all keys manually

```javascript
const iter = map.keys();
let result = iter.next();
while (!result.done) {
  console.log(result.value); // prints key
  result = iter.next();      // move to next
}
```

Output:

```
A
B
C
```

* This is exactly like manually moving a pointer through the keys.
* In LRU cache, we just need the **first key** (`.next().value`) — the oldest one — for eviction.

---



## In JavaScript **`Map` keys can be almost any value**, not just strings. This is one of the big differences from plain objects.

---

## 1️⃣ Allowed key types in `Map`

* **Strings** ✅
* **Numbers** ✅
* **Booleans** ✅
* **Objects** ✅
* **Functions** ✅
* **Symbols** ✅

Basically, **any value can be a key**, and `Map` will treat them differently by reference/value.

---

### Examples:

```javascript
const map = new Map();

// String key
map.set("name", "Alice");

// Number key
map.set(42, "The answer");

// Boolean key
map.set(true, "yes");

// Object key
const objKey = { id: 1 };
map.set(objKey, "object value");

// Function key
const fnKey = () => {};
map.set(fnKey, "function value");

console.log(map.size); // 5
console.log(map.get(42)); // "The answer"
console.log(map.get(objKey)); // "object value"
```

---

## 2️⃣ Key behavior differences

* **Objects/functions**: Keys are compared by **reference**, not content.

```javascript
const a = {x: 1};
map.set(a, "foo");

console.log(map.get({x: 1})); // undefined → different object reference
```

* **Numbers vs Strings**: `Map` treats `42` and `"42"` as different keys.

```javascript
map.set(42, "number");
map.set("42", "string");

console.log(map.get(42));  // "number"
console.log(map.get("42")); // "string"
```

---

### ✅ Key point

Unlike plain objects:

* `Map` keys are **not limited to strings or symbols**.
* It can store **any primitive or object as a key**, which makes it perfect for caches like LRU where your key could be a string, number, or even an object reference.

---
