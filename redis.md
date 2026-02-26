# Basic Data Types (Strings, Lists, Sets)

## Introduction

Redis's true power lies in its native data structures, each optimized for specific use cases. Understanding these structures is fundamental to building efficient applications. Many developers treat Redis as a simple key-value store, missing opportunities for significant performance improvements.

In this lab, we'll explore three fundamental Redis data types: **Strings, Lists, and Sets**. Each solves real-world problems—Strings power caching and session management, Lists enable job queues and activity feeds, and Sets provide efficient tag systems and relationship modeling.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2004/images/1.svg)

We'll build practical examples demonstrating each data type's capabilities: a caching system that reduces database load, a task queue for asynchronous processing, and a tagging system for fast content discovery. We'll also explore Redis's expiration mechanism—a powerful feature that automatically removes stale data.

## Understanding Redis Data Types at a Deep Level
Redis fundamentally differs from traditional databases by providing data structures as first-class primitives. When you store a list in Redis, it's actually a linked list internally; a set is a hash table. This design has profound performance implications—maintaining recent activities requires just push and trim operations in O(1) time, versus complex queries in traditional databases. All Redis operations are atomic, eliminating race conditions without application-level locking.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2004/images/5.svg)


# Redis Strings: More Than Just Text

The **String** data type in Redis is deceptively simple. While it appears to store only text, it is actually a **binary-safe container** capable of holding **any data up to 512 MB**, including text, JSON, serialized objects, compressed data, and images.

Redis Strings are powerful because Redis **interprets the stored value differently depending on the command used**, allowing the same data type to support multiple use cases.

---

## 1. Integer Operations with Strings

Redis Strings can be treated as **integers** and support **atomic arithmetic operations**.

### Supported Commands

* `INCR`
* `DECR`
* `INCRBY`
* `DECRBY`

### Example

```bash
SET page_views 100
INCR page_views
```

Result:

```
101
```

### Use Cases

* Page view counters
* API request counters
* Inventory counts
* Rate limiting

Redis automatically optimizes storage for small integers, making these operations highly efficient.

---

## 2. Float Operations with Strings

Redis Strings also support **floating-point numbers**.

### Supported Command

* `INCRBYFLOAT`

### Example

```bash
SET account_balance 100.75
INCRBYFLOAT account_balance 25.50
```

Result:

```
126.25
```

### Use Cases

* Account balances
* Metrics and measurements
* Scores and weighted values

Internally, the value is stored as a string but parsed and updated as a float during the operation.

---

## 3. Bit Operations with Strings

Redis Strings can be treated as a **sequence of bits** (0s and 1s).
Each bit acts like an **ON/OFF switch**, and Redis allows direct manipulation of individual bits.

A single String can represent **thousands of boolean values** efficiently.

---

### 3.1 Conceptual Model

* A String is a sequence of bytes
* Each byte contains **8 bits**
* Bits are indexed starting from **0**

Example representation:

```
Bit positions:  0 1 2 3 4 5 6 7
Bit values:     0 0 0 0 0 0 0 0
```

---

### 3.2 Setting and Getting Bits

#### Turn a bit ON or OFF

```bash
//user:flags is the name of the string
SETBIT user:flags 0 1
SETBIT user:flags 3 1
```

Resulting bit state:

```
1 0 0 1 0 0 0 0
```

#### Read a bit

```bash
GETBIT user:flags 3
```

Result:

```
1
```

---

### 3.3 Counting Bits

Count how many bits are set to `1`:

```bash
BITCOUNT user:flags
```

Use cases:

* Counting active days
* Counting enabled features
* Counting user actions

---

### 3.4 Bitwise Operations

Redis supports bitwise operations across multiple Strings:

* `AND`
* `OR`
* `XOR`
* `NOT`

Example:

```bash
BITOP AND result key1 key2
GET result
```

This keeps only the bits that are `1` in **both** keys.

Use cases:

* Users active on multiple days
* Features enabled across environments
* Overlapping datasets

---

### 3.5 Practical Example: Daily Login Tracking

Each bit represents a day.

| Day   | Bit Position |
| ----- | ------------ |
| Day 1 | 0            |
| Day 2 | 1            |
| Day 3 | 2            |

User logs in on Day 1 and Day 3:

```bash

// user:login:2026 is the name of the string , nothing else.
SETBIT user:login:2026 0 1
SETBIT user:login:2026 2 1
```

Count login days:

```bash
BITCOUNT user:login:2026
```

Result:

```
2
```

---

## 4. Expiration and Caching

Redis Strings support **expiration (TTL)**, making them ideal for caching.

Example:

```bash
SET session:data "..." EX 3600 # "..." it is the value that we are storing and 3600 is seconds.
```
Check remaining TTL:

```
TTL session:data
```

Remove expiration:

```
PERSIST session:data
```

Set expiration later:

```
EXPIRE session:data 3600
```


Use cases:

* Caching API responses
* Session storage
* Temporary data

---

## 5. Automatic Memory Optimizations

Redis applies internal optimizations automatically:

* Small integers are stored without string overhead
* Small strings use compact encodings
* Bit-level storage is extremely memory efficient

Understanding these optimizations helps in making **better architectural and performance decisions**.

---

## Key Takeaway

**Redis Strings are a multi-purpose data type**:

* Integers → atomic counters
* Floats → precise numeric updates
* Bits → compact, high-performance boolean storage

Despite the name, Redis Strings are one of the most powerful and flexible data types in Redis.


# Redis Lists: Ordered Collections with Queue Semantics

Redis **Lists** are ordered collections of strings designed for **sequential data processing**. Internally, Redis Lists are implemented as **linked lists**, not arrays. This design choice optimizes insertion and deletion at both ends of the list, making Lists ideal for **queues, stacks, and producer–consumer systems**.

---

## 1. Internal Design and Performance

* Redis Lists are implemented as **linked lists**
* Insertions and removals at the **head or tail** run in **O(1)** time
* Access by index (`LINDEX`) runs in **O(n)** time
* Lists are optimized for **sequential access**, not random lookups

This design prioritizes throughput and predictable performance for queue-like workloads.

---

## 2. Core List Operations

### Push Operations

* `LPUSH` – insert element(s) at the head
* `RPUSH` – insert element(s) at the tail

### Pop Operations

* `LPOP` – remove element from the head
* `RPOP` – remove element from the tail

---

## 3. Using Lists as a Queue (FIFO)

### Producer: add jobs to the queue

```bash
RPUSH task_queue job1
RPUSH task_queue job2
```

Queue state:

```
job1 → job2
```

### Consumer: process jobs

```bash
LPOP task_queue
```

Result:

```
job1
```

This pattern ensures **first-in, first-out** processing.

---

## 4. Using Lists as a Stack (LIFO)

```bash
LPUSH stack A
LPUSH stack B
LPUSH stack C
```

Stack state:

```
C → B → A
```

Pop from stack:

```bash
LPOP stack
```

Result:

```
C
```

---

## 5. Fixed-Size Lists (Recent Items Pattern)

Redis Lists are commonly used to store **only the most recent N items**.

```bash
LPUSH recent_events event1
LPUSH recent_events event2
LPUSH recent_events event3
LTRIM recent_events 0 1 # it's index range ,-1 allowed 
```

Final list:

```
event3 → event2
```

### Use cases

* Activity feeds
* Recent logs
* Last N notifications

---

## 6. Accessing Elements by Index (Use Sparingly)

```bash
LINDEX task_queue 0
```

* Retrieves an element by index
* Runs in **O(n)** time
* Should be avoided in hot paths

---

## 7. Blocking Operations (Core Strength of Lists)

Redis Lists support **blocking commands**, which allow consumers to wait efficiently for data.

### Blocking Commands

* `BLPOP`
* `BRPOP`

Blocking commands:

* Do not consume CPU while waiting
* Resume immediately when data arrives
* Enable efficient inter-process communication

---

## 8. Blocking Queue (Producer–Consumer Pattern)

### Redis behavior

```bash
BLPOP task_queue 0
```

* Blocks if the list is empty
* Pops and returns the element the moment it arrives
* Removes the element from the list immediately

Return value:

```text
[ listName, value ]
```

---

## 9. Node.js Worker Example (Real Usage)

Using `ioredis`.

### Worker process

```js
import Redis from "ioredis";

const redis = new Redis();

async function worker() {
  while (true) {
    const [queue, job] = await redis.blpop("task_queue", 0);

    console.log(`Received job from ${queue}:`, job);

    await processJob(job);
  }
}

async function processJob(job) {
  // Business logic
  await new Promise(resolve => setTimeout(resolve, 1000));
}

worker();
```

### Key points

* `BLPOP` blocks the connection efficiently
* No polling or schedulers required
* Worker resumes instantly when a job arrives

---

## 10. Producer Example (Node.js)

```js
import Redis from "ioredis";

const redis = new Redis();

async function addJob(job) {
  try {
    // RPUSH returns the length of the list after the push operation
    const listLength = await redis.rpush("task_queue", job);
    console.log(`Job added. Queue size is now: ${listLength}`);
  } catch (error) {
    console.error("Failed to add job to Redis:", error);
  }
}

// Using an IIFE (Immediately Invoked Function Expression) to allow await at top-level
(async () => {
  await addJob("send_email");
  
  // Optionally close the connection if this is a one-off script
  // redis.disconnect(); 
})();
```

---

## 11. Multiple Workers and Load Balancing

If multiple workers call:

```bash
BLPOP task_queue 0
```

Redis will:

* Wake only **one worker per job**
* Distribute jobs fairly
* Ensure each job is processed once

---

## 12. Important Limitation of List-Based Queues

With `BLPOP`:

* Jobs are removed **before** processing
* If a worker crashes mid-job, the job is lost

### Common solutions

* Use `BRPOPLPUSH` for reliability
* Re-queue failed jobs
* Use Redis Streams for guaranteed delivery

---

## 13. When to Use Redis Lists

Use Lists when you need:

* Simple queues or stacks
* Ordered, sequential processing
* Producer–consumer workflows
* Lightweight background job handling

Avoid Lists when you need:

* Reliable message delivery guarantees
* Complex consumer groups
* Random access or querying

---


Reliable Node.js Redis Worker (2026)
This implementation uses the BLMOVE pattern (Reliable Queue) to ensure no jobs are lost if a worker crashes.

1. Worker & Rescue Script
```js

import Redis from "ioredis";

// We need two connections: 

// One for blocking commands (Worker) and one for standard commands (Rescue/Cleanup)

const workerRedis = new Redis();

const clientRedis = new Redis();

const QUEUE = "task_queue";

const PROCESSING = "tasks_in_progress";

async function worker() {

  console.log("Worker started...");

  while (true) {

    try {

      /**

       * BLMOVE is the modern replacement for BRPOPLPUSH.

       * It atomically moves a job from the main queue to a processing list.

       * If the worker crashes now, the job is safe in 'tasks_in_progress'.

        Argument        Value        Meaning
        -------------/--------------/--------------------------------------------
        Source       /   QUEUE      / Where to take the job from.
        Destination  /   PROCESSING / Where to safely store the job while working.
        Where from?, /   LEFT       / Take from the front of the source list.
        Where to?    /   RIGHT      / Put at the back of the destination list.
        Timeout      /   0          / Block indefinitely until a job is available.

       */

      // Inside the worker loop:
      const job = await workerRedis.blmove(QUEUE, PROCESSING, "LEFT", "RIGHT", 0);

      // Record the start time immediately
      await clientRedis.hset(TIMESTAMPS, job, Date.now());

      console.log(`Processing: ${job}`);

      await processJob(job);

      // When finished, clean up BOTH the list and the timestamp
      await clientRedis.multi()
        .lrem(PROCESSING, 1, job)
        .hdel(TIMESTAMPS, job)
        .exec();

      console.log(`Completed: ${job}`);


    } catch (err) {

      console.error("Worker Error:", err);

    }

  }

}

async function processJob(job) {

  // Your logic here

  await new Promise(res => setTimeout(res, 1000));

}

/**

 * RESCUE SCRIPT

 * This should run periodically (e.g., every 5 minutes).

 * It checks for jobs that have been in the 'PROCESSING' list for too long.

 */

async function rescueStuckJobs() {

  // Logic: In a real app, you might use a Hash to store timestamps.

  // For simplicity, this moves all "orphaned" tasks back to the main queue.

  const stuckJobs = await clientRedis.lrange(PROCESSING, 0, -1);

  

  for (const job of stuckJobs) {

    console.log(`Rescuing stuck job: ${job}`);

    // Move it back to the main queue so a worker can try again

    await clientRedis.lmove(PROCESSING, QUEUE, "RIGHT", "LEFT");

  }

}

worker();

// Run rescue every 30 seconds for demonstration

setInterval(rescueStuckJobs, 30000);

```

2. Producer
```js

import Redis from "ioredis";

const redis = new Redis();

async function addJob(job) {

  // Use await to ensure the job is actually persisted

  await redis.rpush("task_queue", job);

}

addJob("send_email_v1");

```

Key Reliability Points
1. Atomicity: `BLMOVE` ensures a job is never "in between" states. It's either in the queue or in the processing list.

2. Persistence: Even if Node.js crashes, Redis keeps the `tasks_in_progress` list.

3. Recovery: The rescue script acts like a "garbage collector" for failed processes.


# Redis Sets: Unordered Collections with Uniqueness

Redis **Sets** are **unordered collections of unique strings**.

* Uniqueness is **enforced automatically**: adding an existing member has no effect.
* Sets are ideal for storing **distinct items**, such as unique visitors, tags, or relationships where duplicates don’t make sense.

---

## 1. Internal Design and Performance

* Internally implemented as **hash tables**
* Core operations (`SADD`, `SREM`, `SISMEMBER`) run in **O(1) time**, even for millions of members
* Efficient for **membership tests, adding, or removing items**

---

## 2. Core Set Commands

### Adding and Removing Members

```bash
SADD my_set alice
SADD my_set bob
SADD my_set alice  # ignored, already exists
```

```bash
SREM my_set bob
```

### Checking Membership

```bash
SISMEMBER my_set alice   # returns 1 (true)
SISMEMBER my_set bob     # returns 0 (false)
```

### Retrieving Members

```bash
SMEMBERS my_set
```

* Returns all members in **no particular order**

---

## 3. Set Operations

Redis provides **powerful operations** to combine or compare sets efficiently.

### 3.1 Union

Combine all members from multiple sets:

```bash
SUNION setA setB
```

Example:

```
setA = {alice, bob}
setB = {bob, charlie}
SUNION setA setB → {alice, bob, charlie}
```

---

### 3.2 Intersection

Find members common to multiple sets:

```bash
SINTER setA setB
```

Example:

```
setA = {alice, bob}
setB = {bob, charlie}
SINTER setA setB → {bob}
```

* Use case: users who like **both jazz and rock**

---

### 3.3 Difference

Find members in one set but not in others:

```bash
SDIFF setA setB
```

Example:

```
setA = {alice, bob}
setB = {bob, charlie}
SDIFF setA setB → {alice}
```

* Use case: users who liked jazz but **not** rock

---

## 4. Random Members

* `SRANDMEMBER my_set` → return a random member without removing it
* `SPOP my_set` → remove and return a random member

Use cases:

* Random sampling
* Lottery or selection

---

## 5. Counting Members

```bash
SCARD my_set
```

* Returns the total number of unique members in a set
* Useful for counting distinct items like:

  * Unique visitors
  * Unique tags
  * Active users

---

## 6. Real-World Examples

### Example 1: Unique Website Visitors

```bash
SADD unique_visitors user123
SADD unique_visitors user456
SADD unique_visitors user123  # ignored
SCARD unique_visitors         # returns 2
```

### Example 2: Users Who Like Both Jazz and Rock

```bash
SADD jazz alice bob charlie
SADD rock bob charlie dave
SINTER jazz rock  # returns {bob, charlie}
```

### Example 3: Users Who Like Jazz but Not Rock

```bash
SDIFF jazz rock   # returns {alice}
```

---

## 7. Key Advantages of Redis Sets

* **Automatic uniqueness** — no duplicates
* **High performance** — O(1) add, remove, and membership checks
* **Powerful set operations** — union, intersection, difference, all with single commands
* **Random access** — sampling members without scanning

---




In Redis, the way you delete data depends on whether you want to delete the **entire key** (the container) or just **individual items** inside a List, Set, or Hash.

---

### 1. Deleting the Entire Key (Works for all types)

If you want to delete the entire List, Set, or String from memory completely, use the `DEL` or `UNLINK` command.

* `DEL key_name`: Deletes the key immediately (synchronous).
* `UNLINK key_name`: Deletes the key in the background (asynchronous). **Highly recommended for very large Lists or Sets** in production to avoid "freezing" Redis.

---

### 2. Deleting Items inside a LIST

Since Lists are ordered, you usually delete by "value" or by "trimming" the size.

| Action | Command | Explanation |
| --- | --- | --- |
| **Delete by Value** | `LREM my_list 1 "job_1"` | Removes the first occurrence of "job_1" |
| **Delete by Position** | `LPOP my_list` | Removes and returns the first item (Left) |
| **Keep only X items** | `LTRIM my_list 0 99` | Deletes everything except the first 100 items |

---

### 3. Deleting Items inside a SET

Sets are unordered collections of unique strings. You delete specific members.

| Action | Command | Explanation |
| --- | --- | --- |
| **Delete Member** | `SREM my_set "user_123"` | Removes "user_123" from the set |
| **Delete & Return** | `SPOP my_set` | Removes and returns a *random* member |

---

### 4. Deleting Strings

Strings are the simplest type. Since the "value" is the whole thing, you just delete the key.

* **Command:** `DEL my_string_key`

---

### Summary Table for 2026

| Data Type | Command to delete specific item | Command to delete everything |
| --- | --- | --- |
| **String** | N/A (Overwrite with `SET`) | `DEL` / `UNLINK` |
| **List** | `LREM` (by value) | `DEL` / `UNLINK` |
| **Set** | `SREM` (by member) | `DEL` / `UNLINK` |
| **Hash** | `HDEL` (by field) | `DEL` / `UNLINK` |

### Important Logic Note

Remember the **SQLAlchemy `delete-orphan**` logic you use? In Redis, if you delete the last item in a List or Set using `LREM` or `SREM`, Redis **automatically deletes the key itself**. It does not leave an empty "container" behind. Redis is very efficient—if it's empty, it's gone.


## 8. Use Cases

* Tracking **distinct users, tags, or IDs**
* Finding overlaps or differences between groups
* Fast membership checks for large collections
* Random sampling or selection

---

### Understanding Expiration and TTL

One of Redis's most powerful features is automatic key expiration. You can set a Time To Live (TTL) on any key, and Redis automatically removes it when the TTL expires. This feature is essential for caches, sessions, temporary data, and any scenario where data becomes stale or irrelevant over time.

Expiration in Redis is surprisingly sophisticated. Redis doesn't continuously scan all keys checking for expirations—that would be inefficient. Instead, it uses a combination of lazy deletion (checking when a key is accessed) and periodic sampling (randomly checking a subset of keys with expiration set).

You can set expiration when creating a key or add it to existing keys. You can check remaining TTL, remove expiration to make a key permanent, or update expiration times. This flexibility makes expiration a versatile tool for many use cases beyond simple caching.

## Lab Task Description

In this hands-on lab, you will build three practical applications that demonstrate Redis's String, List, and Set data types. Each application addresses a real-world problem and showcases the unique capabilities of its respective data type.

First, you'll implement a caching layer for expensive database operations using Strings. This cache will significantly reduce database load by storing frequently accessed data in Redis with automatic expiration. You'll see how proper caching can transform application performance.

Second, you'll create a task queue system using Lists. This queue will accept tasks via an API and process them asynchronously by worker processes. This pattern is fundamental to scalable architectures, allowing you to decouple request handling from heavy processing.

Third, you'll build a tagging and search system using Sets. Users will be able to tag articles, and you'll implement fast tag-based searching using set operations. This demonstrates how Sets simplify operations that would be complex in traditional databases.

Throughout this lab, you'll also explore expiration and TTL, implementing automatic cache invalidation, temporary task retention, and session-like behavior. By the end, you'll have practical experience with three core Redis data types and understand when to use each one.

## Step-by-Step Solution

### Step 1: Starting Redis with Docker

Before we begin building our application, let's start Redis using Docker. This is the quickest way to get a Redis instance running for development purposes. If you already have Redis running from Lab 01, you can skip this step or stop the existing instance first.

Open your terminal and run the following command to start Redis:

```bash
docker run --name redis-lab2 -p 6379:6379 -d redis:latest
```


Verify the container is running:

```bash
docker ps
```

You should see the `redis-lab2` container listed with status "Up". If you need to check Redis logs to troubleshoot any issues:

```bash
docker logs redis-lab2
```

You should see Redis's startup messages indicating it's ready to accept connections on port 6379.

Test the Redis connection using redis-cli from within the container:

```bash
docker exec -it redis-lab2 redis-cli
```

You'll see the Redis prompt `127.0.0.1:6379>`. Type `PING` and press Enter—Redis should respond with `PONG`. This confirms Redis is running and accepting connections. Type `exit` to leave the Redis CLI.

If you need to stop the Redis container later:

```bash
docker stop redis-lab2
```

To start it again:

```bash
docker start redis-lab2
```

If you want to completely remove the container and start fresh:

```bash
docker stop redis-lab2
docker rm redis-lab2
```

Then you can run the `docker run` command again to create a new container. Now that Redis is running, let's proceed with building our application.

Here are the key **Redis operations** extracted from the code, grouped by data type and use case.

### 1. String Operations (Key-Value Storage)
Used for caching objects, counters, sessions, and simple storage.

| Command                          | Example Usage                                      | Purpose / Note                                                                 |
|----------------------------------|----------------------------------------------------|---------------------------------------------------------------------------------|
| `get(key)`                       | `await redis.get(cacheKey)`                        | Retrieve value (returns string or null if missing).                             |
| `set(key, value)`                | `await redis.set("article:${id}", JSON.stringify(article))` | Store a value permanently.                                                    |
| `setex(key, seconds, value)`     | `await redis.setex(cacheKey, 60, JSON.stringify(userData))` | Set value **with expiration** (e.g., 60-second cache TTL). Very common for caching. |
| `incr(key)`                      | `await redis.incr(counterKey)`                     | Atomically increment a numeric counter (e.g., page views). Safe under concurrency. |
| `del(key)`                       | `await redis.del(sessionKey)`                      | Delete a key (e.g., logout session).                                           |
| `ttl(key)`                       | `await redis.ttl(sessionKey)`                      | Get remaining time-to-live in seconds.                                         |
| `expire(key, seconds)`           | `await redis.expire(notifKey, 2592000)`            | Set expiration on an existing key (e.g., 30 days for notification list).       |

### 2. List Operations (Queues & Feeds)
Used for task queues, activity feeds, recent items.

| Command                          | Example Usage                                      | Purpose / Note                                                                 |
|----------------------------------|----------------------------------------------------|---------------------------------------------------------------------------------|
| `rpush(key, value)`              | `await redis.rpush('task:queue', JSON.stringify(task))` | Push to the **right** end → used with `lpop` for FIFO queue.                  |
| `lpop(key)`                      | `await redis.lpop('task:queue')`                   | Pop from the **left** (front) → consumes next task.                            |
| `lpush(key, value)`              | `await redis.lpush(feedKey, JSON.stringify(activity))` | Push to the **left** → newest items appear first.                             |
| `lrange(key, start, end)`        | `await redis.lrange(feedKey, 0, limit-1)`          | Get a range of items (use `-1` as end for all).                                |
| `llen(key)`                      | `await redis.llen('task:queue')`                   | Get current list length.                                                       |
| `ltrim(key, start, end)`         | `await redis.ltrim(feedKey, 0, 49)`                | Keep only the first N items → creates capped "recent" feeds.                   |

### 3. Set Operations (Unique Collections)
Used for tags, followers, many-to-many relationships.

```
| Command                        | Example Usage                                      | Purpose / Note                                                                  |
|--------------------------------|----------------------------------------------------|---------------------------------------------------------------------------------|
| sadd(key, member...)           | await redis.sadd(`tag:${tag}:articles`, id)        | Add members to a set (duplicates ignored → unique).                             |
| smembers(key)                  | await redis.smembers(`tag:${tag}:articles`)        | Get all members of a set.                                                       |
| scard(key)                     | await redis.scard(followersKey)                    | Get member count (e.g., follower count).                                        |
| sismember(key, member)         | await redis.sismember(followersKey, followerId)    | Fast check if a member exists (e.g., "is following?").                          |
| sinter(key1, key2, ...)        | await redis.sinter(...tagKeys)                     | Intersection → items common to **all** sets (e.g., articles with ALL tags).     |
| sunion(key1, key2, ...)        | await redis.sunion(...tagKeys)                     | Union → items in **any** of the sets (e.g., articles with ANY tag).             |

In Node.js, if you pass an array directly: await redis.sinter(tagKeys); ❌ Error: Redis thinks you are passing one key that happens to look like an array string.

By using the spread operator: await redis.sinter(...tagKeys); ✅ Success: JavaScript spreads the array into separate string arguments that Redis understands
```

### Common Patterns Demonstrated

- **Caching**: `GET` → miss → fetch → `SETEX` (with TTL)
- **Counters**: `INCR` for atomic, concurrent-safe counting
- **Sessions**: JSON string + `SETEX` for auto-expiry
- **Task Queue**: `RPUSH` + `LPOP` → simple FIFO
- **Recent Feeds/Notifications**: `LPUSH` + `LTRIM` → capped latest items
- **Tagging**: Reverse index sets (`tag:x:articles`) + per-item tag set
- **Relationships**: Bidirectional sets + `SINTER` for mutual followers

Copy and paste this directly into your `.md` file — the tables should now render perfectly!
### Step 2: Setting Up the Project Structure

Let's start by creating a new project for this lab. We'll build on the knowledge from Lab 01 but create a fresh structure focused on demonstrating different data types. Create a new directory:

```bash
mkdir redis-datatypes-lab
cd redis-datatypes-lab
```

Initialize the Node.js project and install dependencies:

```bash
npm init -y
npm install express ioredis
```

Create the basic project structure:

```bash
touch server.js redis.js
mkdir routes
touch routes/strings.js routes/lists.js routes/sets.js
```

This structure separates concerns—each route file will handle one data type. Let's create the Redis connection module first. Open `redis.js`:

```javascript
const Redis = require('ioredis');

const redis = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
  maxRetriesPerRequest: 3
});

redis.on('connect', () => {
  console.log('✓ Redis client connected');
});

redis.on('ready', () => {
  console.log('✓ Redis client ready');
});

redis.on('error', (err) => {
  console.error('✗ Redis error:', err.message);
});

// Use quit() for a clean exit (finishes pending work).
// Use disconnect() only as a "kill switch" if quit() fails.

process.on('SIGINT', async () => {
  await redis.quit();
  process.exit(0);
});

module.exports = redis;
```

This is similar to Lab 01 but streamlined for our new project. Now let's create the main server file. Open `server.js`:

```javascript
const express = require('express');
const redis = require('./redis');

const stringsRoutes = require('./routes/strings');
const listsRoutes = require('./routes/lists');
const setsRoutes = require('./routes/sets');

const app = express();
const PORT = 5000;

app.use(express.json());

// Mount routes
app.use('/api/strings', stringsRoutes);
app.use('/api/lists', listsRoutes);
app.use('/api/sets', setsRoutes);

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    redis: redis.status 
  });
});

app.listen(PORT, () => {
  console.log(`\n🚀 Server running on http://localhost:${PORT}`);
  console.log(`📊 Health: http://localhost:${PORT}/health\n`);
});
```

This server mounts three route modules, each focused on a specific data type. This modular approach keeps our code organized as complexity grows.

### Step 3: Implementing String Operations with Caching

Now let's implement practical String operations. We'll build a caching system that simulates expensive database queries. Open `routes/strings.js`:

`app.use(express.json())` is actually middleware for incoming requests **(parsing the body of a POST/PUT request)** . It doesn't affect the data you are sending out.

Here is the exact breakdown of why `JSON.parse ` is necessary in your code:

1. The Redis Factor

Redis is a "string-safe" store. When you use redis.setex, you have to turn your object into a string using JSON.stringify(). When you redis.get() it later, Redis returns a string, not a JavaScript object.

2. How res.json() handles data

When you pass something to res.json(data), Express looks at the data:

If it's an Object: Express stringifies it for you and sets the header to application/json.

If it's a String: Express assumes it is already a JSON string and sends it as-is.

So, if you didn't use JSON.parse(cachedUser), your response would look like this:

JSON


```JSON
{

  "source": "cache",

  "data": "{\"id\":123,\"name\":\"John Doe\"}" 

}
```
Notice the data field is a escaped string. By using JSON.parse(), you turn it back into a real object so that the final response is a cleanly nested JSON object:

```JSON

{

  "source": "cache",

  "data": {

    "id": 123,

    "name": "John Doe"

  }

}
```

```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Simulate an expensive database query
function expensiveDatabaseQuery(userId) {
  // In reality, this would query a database
  // We'll simulate with a delay and mock data
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve({
        id: userId,
        name: `User ${userId}`,
        email: `user${userId}@example.com`,
        profile: {
          bio: 'Lorem ipsum dolor sit amet',
          interests: ['coding', 'redis', 'nodejs']
        },
        fetchedAt: new Date().toISOString()
      });
    }, 1000); // 1 second delay to simulate slow query
  });
}

// Get user with caching
router.get('/users/:id', async (req, res) => {
  try {
    const userId = req.params.id;
    const cacheKey = `user:${userId}`;

    // Try to get from cache first
    const cachedUser = await redis.get(cacheKey);

    if (cachedUser) {
      console.log(`Cache HIT for user ${userId}`);
      return res.json({
        source: 'cache',
        data: JSON.parse(cachedUser)
      });
    }

    // Cache miss - fetch from "database"
    console.log(`Cache MISS for user ${userId} - fetching from database`);
    const userData = await expensiveDatabaseQuery(userId);

    // Store in cache with 60 second expiration
    await redis.setex(cacheKey, 60, JSON.stringify(userData));

    res.json({
      source: 'database',
      data: userData
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Page view counter
router.post('/pageviews/:page', async (req, res) => {
  try {
    const page = req.params.page;
    const counterKey = `pageviews:${page}`;

    // Atomically increment counter
    const newCount = await redis.incr(counterKey);

    res.json({
      page,
      views: newCount
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get page view count
router.get('/pageviews/:page', async (req, res) => {
  try {
    const page = req.params.page;
    const counterKey = `pageviews:${page}`;

    const count = await redis.get(counterKey);

    res.json({
      page,
      views: parseInt(count) || 0
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Session management
router.post('/sessions', async (req, res) => {
  try {
    const { userId, data } = req.body;

    if (!userId) {
      return res.status(400).json({ error: 'userId required' });
    }

    // Generate session ID
    const sessionId = `sess_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const sessionKey = `session:${sessionId}`;

    // Store session with 30 minute expiration
    //If data is missing, it stores undefined BUT WE WANT   empty object {}, so we wrote data: data|| {} , not only data
    const sessionData = {
      userId,
      data: data || {},
      createdAt: new Date().toISOString()
    };

    await redis.setex(sessionKey, 1800, JSON.stringify(sessionData));

    res.json({
      sessionId,
      expiresIn: 1800
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get session
router.get('/sessions/:id', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const sessionKey = `session:${sessionId}`;

    const sessionData = await redis.get(sessionKey);

    if (!sessionData) {
      return res.status(404).json({ error: 'Session not found or expired' });
    }

    // Get remaining TTL
    const ttl = await redis.ttl(sessionKey);

    res.json({
      sessionId,
      data: JSON.parse(sessionData),
      expiresIn: ttl
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Delete session (logout)
router.delete('/sessions/:id', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const sessionKey = `session:${sessionId}`;

    const deleted = await redis.del(sessionKey);

    if (!deleted) {
      return res.status(404).json({ error: 'Session not found' });
    }

    res.json({ message: 'Session deleted successfully' });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```

This module demonstrates several critical String operations. The caching endpoint shows how Redis dramatically improves performance for expensive operations. The first request takes about 1 second (simulating a slow database query), but subsequent requests return instantly from cache. The `SETEX` command combines SET and EXPIRE in a single atomic operation.

The page view counter demonstrates atomic increment operations. Multiple concurrent requests can safely increment the same counter without race conditions. This is crucial for analytics, rate limiting, and distributed counting scenarios.

The session management endpoints show how expiration enables automatic cleanup. Sessions expire after 30 minutes without any background jobs or cleanup processes. The `TTL` command lets you check remaining time, useful for showing "session expires in X minutes" messages to users.

### Step 4: Implementing List Operations with Task Queues

Now let's implement List operations by building a task queue system. Open `routes/lists.js`:

```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Add task to queue
router.post('/tasks', async (req, res) => {
  try {
    const { type, payload } = req.body;

    if (!type || !payload) {
      return res.status(400).json({ error: 'type and payload required' });
    }

    const task = {
      id: `task_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      type,
      payload,
      createdAt: new Date().toISOString(),
      status: 'pending'
    };

    // Push task to the right end of the queue
    await redis.rpush('task:queue', JSON.stringify(task));

    // Get queue length
    const queueLength = await redis.llen('task:queue');

    res.status(201).json({
      message: 'Task added to queue',
      task,
      queueLength
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get next task from queue (worker simulation)
router.get('/tasks/next', async (req, res) => {
  try {
    // Pop task from the left end of the queue
    const taskData = await redis.lpop('task:queue');

    if (!taskData) {
      return res.json({
        message: 'Queue is empty',
        task: null
      });
    }

    const task = JSON.parse(taskData);

    // In a real system, you'd mark this task as processing
    // and add it to a processing list

    res.json({
      message: 'Task retrieved',
      task
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get queue status
router.get('/tasks/queue', async (req, res) => {
  try {
    const queueLength = await redis.llen('task:queue');
    
    // Get first 10 tasks without removing them
    const tasks = await redis.lrange('task:queue', 0, 9);
    const parsedTasks = tasks.map(task => JSON.parse(task));

    res.json({
      queueLength,
      preview: parsedTasks
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Activity feed - add activity
router.post('/feed/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const { activity } = req.body;

    if (!activity) {
      return res.status(400).json({ error: 'activity required' });
    }

    const feedKey = `feed:${userId}`;
    const activityItem = {
      activity,
      timestamp: new Date().toISOString()
    };

    // Add to the left (most recent)
    await redis.lpush(feedKey, JSON.stringify(activityItem));

    // Keep only the 50 most recent activities
    await redis.ltrim(feedKey, 0, 49);

    res.json({
      message: 'Activity added',
      activity: activityItem
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get activity feed
router.get('/feed/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const feedKey = `feed:${userId}`;
    const limit = parseInt(req.query.limit) || 10;

    const activities = await redis.lrange(feedKey, 0, limit - 1);
    const parsedActivities = activities.map(item => JSON.parse(item));

    res.json({
      userId,
      activities: parsedActivities,
      count: parsedActivities.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Recent notifications with expiration
router.post('/notifications/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const { message } = req.body;

    if (!message) {
      return res.status(400).json({ error: 'message required' });
    }

    const notifKey = `notifications:${userId}`;
    const notification = {
      message,
      timestamp: new Date().toISOString(),
      read: false
    };

    await redis.lpush(notifKey, JSON.stringify(notification));
    await redis.ltrim(notifKey, 0, 99); // Keep last 100
    
    // Set expiration on the list (30 days)
    await redis.expire(notifKey, 2592000);

    res.json({
      message: 'Notification added',
      notification
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get notifications
router.get('/notifications/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const notifKey = `notifications:${userId}`;

    const notifications = await redis.lrange(notifKey, 0, -1);
    const parsedNotifications = notifications.map(n => JSON.parse(n));

    res.json({
      userId,
      notifications: parsedNotifications,
      count: parsedNotifications.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2004/images/2.svg)

This module showcases Redis Lists' power for queue and feed implementations. The task queue uses `RPUSH` to add tasks to the right end and `LPOP` to remove from the left, creating a FIFO (First In, First Out) queue. This is the foundation of job processing systems like Bull and BullMQ.

The activity feed demonstrates how `LTRIM` maintains fixed-size lists automatically. Every time you add an activity, `LTRIM` ensures only the 50 most recent items remain. This pattern is perfect for timelines, recent activity feeds, or any scenario where you only care about recent items.

The notification system combines Lists with expiration. The entire list expires after 30 days, automatically cleaning up old notifications. This shows that expiration works on any Redis key, not just strings.

### Step 5: Implementing Set Operations with Tagging

Finally, let's implement Set operations through a tagging system. Open `routes/sets.js`:

```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Create article with tags
router.post('/articles', async (req, res) => {
  try {
    const { id, title, content, tags } = req.body;

    if (!id || !title || !tags || !Array.isArray(tags)) {
      return res.status(400).json({ 
        error: 'id, title, and tags array required' 
      });
    }

    const article = {
      id,
      title,
      content: content || '',
      createdAt: new Date().toISOString()
    };

    // Store article data
    await redis.set(`article:${id}`, JSON.stringify(article));

    // Store article ID in each tag's set
    for (const tag of tags) {
      await redis.sadd(`tag:${tag}:articles`, id);
    }

    // Store tags for this article
    await redis.sadd(`article:${id}:tags`, ...tags);

    res.status(201).json({
      message: 'Article created',
      article,
      tags
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get articles by tag
router.get('/tags/:tag/articles', async (req, res) => {
  try {
    const tag = req.params.tag;
    const tagKey = `tag:${tag}:articles`;

    // Get all article IDs with this tag
    const articleIds = await redis.smembers(tagKey);

    if (articleIds.length === 0) {
      return res.json({
        tag,
        articles: [],
        count: 0
      });
    }

    // Fetch article data for each ID
    const articles = await Promise.all(
      articleIds.map(async (id) => {
        const data = await redis.get(`article:${id}`);
        const tags = await redis.smembers(`article:${id}:tags`);
        return {
          ...JSON.parse(data),
          tags
        };
      })
    );

    res.json({
      tag,
      articles,
      count: articles.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get article with its tags
router.get('/articles/:id', async (req, res) => {
  try {
    const articleId = req.params.id;
    const articleKey = `article:${articleId}`;

    const articleData = await redis.get(articleKey);

    if (!articleData) {
      return res.status(404).json({ error: 'Article not found' });
    }

    const tags = await redis.smembers(`article:${articleId}:tags`);

    res.json({
      article: JSON.parse(articleData),
      tags
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Find articles with ALL specified tags (intersection)
router.post('/articles/search/all', async (req, res) => {
  try {
    const { tags } = req.body;

    if (!tags || !Array.isArray(tags) || tags.length === 0) {
      return res.status(400).json({ error: 'tags array required' });
    }

    // Get intersection of all tag sets
    const tagKeys = tags.map(tag => `tag:${tag}:articles`);
    const articleIds = await redis.sinter(...tagKeys);

    if (articleIds.length === 0) {
      return res.json({
        searchTags: tags,
        articles: [],
        count: 0
      });
    }

    const articles = await Promise.all(
      articleIds.map(async (id) => {
        const data = await redis.get(`article:${id}`);
        const articleTags = await redis.smembers(`article:${id}:tags`);
        return {
          ...JSON.parse(data),
          tags: articleTags
        };
      })
    );

    res.json({
      searchTags: tags,
      matchType: 'all',
      articles,
      count: articles.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Find articles with ANY specified tags (union)
router.post('/articles/search/any', async (req, res) => {
  try {
    const { tags } = req.body;

    if (!tags || !Array.isArray(tags) || tags.length === 0) {
      return res.status(400).json({ error: 'tags array required' });
    }

    // Get union of all tag sets
    const tagKeys = tags.map(tag => `tag:${tag}:articles`);
    const articleIds = await redis.sunion(...tagKeys);

    if (articleIds.length === 0) {
      return res.json({
        searchTags: tags,
        articles: [],
        count: 0
      });
    }

    const articles = await Promise.all(
      articleIds.map(async (id) => {
        const data = await redis.get(`article:${id}`);
        const articleTags = await redis.smembers(`article:${id}:tags`);
        return {
          ...JSON.parse(data),
          tags: articleTags
        };
      })
    );

    res.json({
      searchTags: tags,
      matchType: 'any',
      articles,
      count: articles.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add user to followers set
router.post('/users/:userId/followers/:followerId', async (req, res) => {
  try {
    const { userId, followerId } = req.params;
    const followersKey = `user:${userId}:followers`;
    const followingKey = `user:${followerId}:following`;

    // Add to both sets
    await redis.sadd(followersKey, followerId);
    await redis.sadd(followingKey, userId);

    const followerCount = await redis.scard(followersKey);

    res.json({
      message: 'Follower added',
      userId,
      followerId,
      followerCount
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get mutual followers
router.get('/users/:userId1/mutual/:userId2', async (req, res) => {
  try {
    const { userId1, userId2 } = req.params;

    const followers1Key = `user:${userId1}:followers`;
    const followers2Key = `user:${userId2}:followers`;

    // Get intersection (mutual followers)
    const mutualFollowers = await redis.sinter(followers1Key, followers2Key);

    res.json({
      user1: userId1,
      user2: userId2,
      mutualFollowers,
      count: mutualFollowers.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Check if user is follower
router.get('/users/:userId/followers/:followerId/check', async (req, res) => {
  try {
    const { userId, followerId } = req.params;
    const followersKey = `user:${userId}:followers`;

    const isFollower = await redis.sismember(followersKey, followerId);

    res.json({
      userId,
      followerId,
      isFollower: isFollower === 1
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```

This module demonstrates Sets' power for relationships and tagging. The article tagging system uses Sets to maintain bidirectional relationships: which articles have a given tag, and which tags an article has. This pattern is fundamental to many applications.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2004/images/4.svg)

The search endpoints showcase set operations. `SINTER` finds articles with all specified tags (intersection), while `SUNION` finds articles with any specified tag (union). These operations would require complex SQL queries with multiple joins in a relational database, but in Redis they're single commands.

The follower system demonstrates social graph operations. Finding mutual followers is a simple set intersection. Checking if someone is a follower uses `SISMEMBER`, which runs in O(1) time regardless of follower count.

### Step 6: Testing the Complete Application

Now let's start the application and test all the functionality we've built. Make sure Redis is running in Docker:

```bash
# Check if container is running
docker ps

# If not running, start it
docker start redis-lab2

# Start the application
node server.js
```

You should see the Redis connection messages and the server starting on port 5000. Now let's test each data type systematically.

**Testing String Operations (Caching):**

First, test the caching system. Make a request for user 1:

```bash
curl http://localhost:5000/api/strings/users/1
```

This request takes about 1 second because it simulates a slow database query. Notice the response says `"source": "database"`. Now make the same request again immediately:

```bash
curl http://localhost:5000/api/strings/users/1
```

This returns instantly, and the response says `"source": "cache"`. Check your server logs—you'll see "Cache HIT" messages. Make requests for the same user multiple times within 60 seconds, and they'll all hit the cache. Wait 61 seconds and try again—the cache expires, and the next request goes to the "database."

Test the page view counter:

```bash
curl -X POST http://localhost:5000/api/strings/pageviews/homepage
curl -X POST http://localhost:5000/api/strings/pageviews/homepage
curl -X POST http://localhost:5000/api/strings/pageviews/homepage
```

Each request increments the counter. Now check the count:

```bash
curl http://localhost:5000/api/strings/pageviews/homepage
```

You should see `"views": 3`. Even if you made these requests simultaneously from multiple terminals, the count would be accurate because Redis's INCR operation is atomic.

Test session management:

```bash
curl -X POST http://localhost:5000/api/strings/sessions \
  -H "Content-Type: application/json" \
  -d '{"userId": "user123", "data": {"role": "admin"}}'
```

Copy the sessionId from the response, then retrieve the session:

```bash
curl http://localhost:5000/api/strings/sessions/YOUR_SESSION_ID
```

Notice the `expiresIn` field showing remaining seconds. Wait a minute and check again—the number decreases. After 30 minutes, the session automatically expires.

**Testing List Operations (Queues):**

Add some tasks to the queue:

```bash
curl -X POST http://localhost:5000/api/lists/tasks \
  -H "Content-Type: application/json" \
  -d '{"type": "email", "payload": {"to": "user@example.com", "subject": "Welcome"}}'

curl -X POST http://localhost:5000/api/lists/tasks \
  -H "Content-Type: application/json" \
  -d '{"type": "thumbnail", "payload": {"imageId": "img123", "size": "200x200"}}'

curl -X POST http://localhost:5000/api/lists/tasks \
  -H "Content-Type: application/json" \
  -d '{"type": "notification", "payload": {"userId": "user456", "message": "New comment"}}'
```

Check the queue status:

```bash
curl http://localhost:5000/api/lists/tasks/queue
```

You'll see all three tasks waiting to be processed. Now simulate a worker processing tasks:

```bash
curl http://localhost:5000/api/lists/tasks/next
curl http://localhost:5000/api/lists/tasks/next
curl http://localhost:5000/api/lists/tasks/next
```

Each request retrieves and removes one task in FIFO order. The fourth request returns an empty queue message.

Test the activity feed:

```bash
curl -X POST http://localhost:5000/api/lists/feed/user123 \
  -H "Content-Type: application/json" \
  -d '{"activity": "Posted a new article"}'

curl -X POST http://localhost:5000/api/lists/feed/user123 \
  -H "Content-Type: application/json" \
  -d '{"activity": "Commented on a photo"}'

curl -X POST http://localhost:5000/api/lists/feed/user123 \
  -H "Content-Type: application/json" \
  -d '{"activity": "Liked a video"}'
```

Retrieve the feed:

```bash
curl http://localhost:5000/api/lists/feed/user123
```

Activities appear in reverse chronological order (most recent first). Add 50 more activities, and the oldest ones automatically disappear due to LTRIM.

**Testing Set Operations (Tagging):**

Create some articles with tags:

```bash
curl -X POST http://localhost:5000/api/sets/articles \
  -H "Content-Type: application/json" \
  -d '{"id": "1", "title": "Introduction to Redis", "content": "Redis is amazing", "tags": ["redis", "database", "tutorial"]}'

curl -X POST http://localhost:5000/api/sets/articles \
  -H "Content-Type: application/json" \
  -d '{"id": "2", "title": "Node.js Best Practices", "content": "Learn Node", "tags": ["nodejs", "javascript", "tutorial"]}'

curl -X POST http://localhost:5000/api/sets/articles \
  -H "Content-Type: application/json" \
  -d '{"id": "3", "title": "Redis Data Types", "content": "Deep dive", "tags": ["redis", "advanced", "tutorial"]}'
```

Find articles with the "redis" tag:

```bash
curl http://localhost:5000/api/sets/tags/redis/articles
```

You get articles 1 and 3. Find articles with the "tutorial" tag:

```bash
curl http://localhost:5000/api/sets/tags/tutorial/articles
```

You get all three articles. Now test set intersection—find articles with BOTH "redis" AND "tutorial" tags:

```bash
curl -X POST http://localhost:5000/api/sets/articles/search/all \
  -H "Content-Type: application/json" \
  -d '{"tags": ["redis", "tutorial"]}'
```

You get articles 1 and 3 (both have both tags). Test set union—find articles with "redis" OR "nodejs":

```bash
curl -X POST http://localhost:5000/api/sets/articles/search/any \
  -H "Content-Type: application/json" \
  -d '{"tags": ["redis", "nodejs"]}'
```

You get all three articles (1 and 3 have redis, 2 has nodejs).

Test the follower system:

```bash
curl -X POST http://localhost:5000/api/sets/users/alice/followers/bob
curl -X POST http://localhost:5000/api/sets/users/alice/followers/charlie
curl -X POST http://localhost:5000/api/sets/users/bob/followers/charlie
curl -X POST http://localhost:5000/api/sets/users/bob/followers/david
```

Now find mutual followers between alice and bob:

```bash
curl http://localhost:5000/api/sets/users/alice/mutual/bob
```

You get charlie (the only person following both). Check if charlie follows alice:

```bash
curl http://localhost:5000/api/sets/users/alice/followers/charlie/check
```

Returns `"isFollower": true`.

### Step 7: Exploring Operations in Redis CLI

While your application is running, open another terminal and connect to Redis:

```bash
docker exec -it redis-lab2 redis-cli
```

Let's examine the data structures directly. Check all keys:

```bash
KEYS *
```

You'll see keys organized by prefix: `user:`, `article:`, `tag:`, `session:`, etc. This naming convention makes keys self-documenting.

Examine a cached user:

```bash
GET user:1
```

You see the JSON string. Check its TTL:

```bash
TTL user:1
```

This shows remaining seconds until expiration. If you see `-1`, the key has no expiration. If you see `-2`, the key doesn't exist (already expired).

Examine a list (queue):

```bash
LLEN task:queue
```

Shows how many tasks are queued. View the queue contents without removing items:

```bash
LRANGE task:queue 0 -1
```

The `-1` means "to the end of the list." You see all queued tasks.

Examine a set (article tags):

```bash
SMEMBERS article:1:tags
```

Shows all tags for article 1. Check how many articles have the "redis" tag:

```bash
SCARD tag:redis:articles
```

`SCARD` returns the cardinality (size) of the set. See which articles have the tag:

```bash
SMEMBERS tag:redis:articles
```

Try a set operation manually:

```bash
SINTER tag:redis:articles tag:tutorial:articles
```

This performs the same intersection operation your API does, showing article IDs that have both tags.

Monitor all Redis operations in real-time:

```bash
MONITOR
```

Now make API requests in another terminal. You'll see every Redis command executed—SET, GET, SADD, LPUSH, etc. This is invaluable for debugging and understanding how your application uses Redis. Press Ctrl+C to stop monitoring.

Check memory usage:

```bash
INFO memory
```

You'll see detailed statistics. Notice that even with all this data, Redis uses minimal memory. Check overall statistics:

```bash
INFO stats
```

Shows total commands processed, connections received, and other metrics.

### Step 8: Understanding Performance Characteristics

Let's explore the performance implications of our design choices. Create a script to measure cache performance. Create `test-performance.js`:

```javascript
const axios = require('axios');

async function testCachePerformance() {
  const userId = Math.floor(Math.random() * 100);
  const url = `http://localhost:5000/api/strings/users/${userId}`;

  // First request (cache miss)
  const start1 = Date.now();
  const response1 = await axios.get(url);
  const duration1 = Date.now() - start1;

  console.log(`First request (${response1.data.source}): ${duration1}ms`);

  // Second request (cache hit)
  const start2 = Date.now();
  const response2 = await axios.get(url);
  const duration2 = Date.now() - start2;

  console.log(`Second request (${response2.data.source}): ${duration2}ms`);
  console.log(`Speedup: ${(duration1 / duration2).toFixed(2)}x faster`);
}

testCachePerformance();
```

Install axios: `npm install axios`, then run:

```bash
node test-performance.js
```

You'll see the first request takes ~1000ms, while the second takes just a few milliseconds—a 100-200x speedup! This demonstrates why caching is so powerful.

Now let's test Set operations performance. Update the script:

```javascript
const axios = require('axios');

async function testSetPerformance() {
  // Create 1000 articles with random tags
  console.log('Creating 1000 articles...');
  const tags = ['redis', 'nodejs', 'javascript', 'database', 'tutorial', 'advanced'];
  
  for (let i = 1; i <= 1000; i++) {
    const randomTags = tags
      .sort(() => Math.random() - 0.5)
      .slice(0, 3);
    
    await axios.post('http://localhost:5000/api/sets/articles', {
      id: i.toString(),
      title: `Article ${i}`,
      content: `Content ${i}`,
      tags: randomTags
    });

    if (i % 100 === 0) console.log(`  Created ${i} articles`);
  }

  // Test search performance
  console.log('\nTesting search performance...');
  
  const start = Date.now();
  const response = await axios.post('http://localhost:5000/api/sets/articles/search/all', {
    tags: ['redis', 'tutorial']
  });
  const duration = Date.now() - start;

  console.log(`Found ${response.data.count} articles in ${duration}ms`);
  console.log(`That's ${(response.data.count / duration).toFixed(2)} articles per millisecond!`);
}

testSetPerformance();
```

Run this script. Even with 1000 articles, the set intersection completes in just a few milliseconds. This demonstrates how Redis's native data structures dramatically outperform application-level implementations.

### Step 9: Implementing Expiration Patterns

Let's explore more sophisticated expiration patterns. Add this endpoint to `routes/strings.js`:

```javascript
// Rate limiting with sliding window
router.post('/ratelimit/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const limit = 10; // 10 requests
    const window = 60; // per 60 seconds
    const key = `ratelimit:${userId}`;

    // Get current count
    const current = await redis.get(key);

    if (current && parseInt(current) >= limit) {
      const ttl = await redis.ttl(key);
      return res.status(429).json({
        error: 'Rate limit exceeded',
        retryAfter: ttl
      });
    }

    // Increment counter
    const newCount = await redis.incr(key);

    // Set expiration on first request
    if (newCount === 1) {
      await redis.expire(key, window);
    }

    const remaining = limit - newCount;

    res.json({
      message: 'Request accepted',
      remaining,
      resetsIn: await redis.ttl(key)
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
```

This implements a simple rate limiter. Test it by making rapid requests:

```bash
for i in {1..15}; do
  curl -X POST http://localhost:5000/api/strings/ratelimit/user123
  echo ""
done
```

The first 10 requests succeed, then you get rate limit errors showing when you can retry. After 60 seconds, the counter resets automatically.

Add another pattern for temporary data:

```javascript
// One-time password (OTP) with expiration
router.post('/otp/generate/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const otp = Math.floor(100000 + Math.random() * 900000); // 6-digit code
    const key = `otp:${userId}`;

    // Store OTP with 5-minute expiration
    await redis.setex(key, 300, otp.toString());

    res.json({
      message: 'OTP generated',
      otp, // In production, send via SMS/email instead
      expiresIn: 300
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Verify OTP
router.post('/otp/verify/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const { otp } = req.body;
    const key = `otp:${userId}`;

    const storedOtp = await redis.get(key);

    if (!storedOtp) {
      return res.status(400).json({ error: 'OTP expired or not found' });
    }

    if (storedOtp !== otp.toString()) {
      return res.status(400).json({ error: 'Invalid OTP' });
    }

    // Delete OTP after successful verification (single use)
    await redis.del(key);

    res.json({ message: 'OTP verified successfully' });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
```

This implements a one-time password system. Generate an OTP:

```bash
curl -X POST http://localhost:5000/api/strings/otp/generate/user123
```

Copy the OTP from the response and verify it:

```bash
curl -X POST http://localhost:5000/api/strings/otp/verify/user123 \
  -H "Content-Type: application/json" \
  -d '{"otp": "YOUR_OTP_HERE"}'
```

Try verifying again with the same OTP—it fails because the OTP was deleted after first use. Wait 5 minutes and try to verify—it fails because the OTP expired.

## Conclusion

In this lab, we've explored three fundamental Redis data types—Strings, Lists, and Sets—building practical applications that demonstrate their real-world utility.

**Strings** proved versatile with caching (100x performance gains), atomic counters, session management, and rate limiting. **Lists** excelled at ordered collections with O(1) operations, perfect for task queues and activity feeds. **Sets** showcased uniqueness constraints and powerful set operations (union, intersection, difference) for tagging and social graphs.




# Advanced Data Types (Hashes, Sorted Sets)

## Introduction

Consider our user caching system: we stored entire user objects as JSON strings. To update a single field like email, we must retrieve the entire JSON, parse it, modify it, serialize it back, and store the whole thing againâ€”inefficient and prone to race conditions. Similarly, displaying articles by recency or finding top contributors had no elegant solution. Lists don't allow efficient insertion at arbitrary positions, and Sets are unordered.

This is where Redis's advanced data types shine. **Hashes** store objects as field-value pairs, allowing individual field updates without touching the rest of the object. **Sorted Sets** maintain elements ordered by score, enabling leaderboards, time-series data, and priority queues with logarithmic time complexity.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2005/images/1.svg)

In this lab, we'll explore Hashes and Sorted Sets in depth. We'll rebuild our user system using Hashes for efficient field-level operations, implement a real-time leaderboard, create a priority task queue, and build a time-series analytics system.



You'll understand when these structures provide significant advantagesâ€”how Hashes eliminate JSON serialization overhead and enable atomic field updates, and how Sorted Sets make operations like "top 10 users" or "scores between X and Y" trivially easy.

## Understanding Hashes and Sorted Sets

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2005/images/4.svg)

### Hashes: Objects Without the JSON Overhead

Redis Hashes are maps between string field names and string field values. They're the perfect data structure for representing objects. While you could store objects as JSON strings (as we did in Lab 02), Hashes offer several compelling advantages that make them the preferred choice for object storage in Redis.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2005/images/3.svg)

First, Hashes allow field-level operations. Instead of retrieving, parsing, modifying, serializing, and storing an entire object, you can directly get or set individual fields. This is not just more convenientâ€”it's significantly more efficient. Retrieving a single field from a Hash with millions of fields still completes in O(1) time.

Second, Hashes provide atomic field updates without race conditions. When two clients simultaneously update different fields of the same object, both operations succeed cleanly. With JSON strings, you'd need application-level locking or optimistic concurrency control to prevent one update from overwriting the other.

Third, Hashes use less memory than equivalent JSON strings for objects with multiple fields. Redis uses a special encoding called ziplist for small hashes, which stores fields and values in a compact, contiguous memory block. This optimization can reduce memory usage by 5-10x compared to JSON strings.

Fourth, Hashes support powerful field-level operations. You can atomically increment numeric fields (perfect for counters), check if fields exist, get multiple fields in a single round trip, or retrieve all fields and values at once. These operations would require custom parsing logic with JSON strings.

Hashes are implemented as hash tables internally, providing O(1) average time complexity for field access, regardless of the number of fields in the Hash. This makes them suitable even for objects with hundreds or thousands of fields.

### Sorted Sets: Order Without the Overhead

### 1. What a Sorted Set Is

* A Redis **Sorted Set** stores **unique members**, each with an associated **score** (a floating-point number).
* Members are **automatically kept in ascending order by score**.
* If multiple members share the same score, they are ordered **lexicographically (alphabetically)**.

---

### 2. How Redis Maintains Order

Redis uses a **dual internal structure**:

* **Hash table** → O(1) lookup by member
* **Skip list** → O(log N) ordered operations

This ensures both fast access and efficient sorting.

---

### 3. Performance Characteristics

Sorted Sets provide highly efficient ordered operations:

* **Top N elements**:

  * `O(log N + N)`
* **Range queries by score**:

  * `O(log N + M)` where M = number of results
* **Rank of a member**:

  * `O(log N)`
* **Insertion / update**:

  * `O(log N)`

These are much faster than application-side sorting (`O(N log N)`).

---

### 4. Why Sorted Sets Are Powerful

They enable operations that are complex or slow with other structures:

* Leaderboards
* Priority queues
* Time-series indexing
* Ranking systems
* Rate-limiting and scheduling
* Ordered pagination

---

### 5. Meaning of the Score (Use-Case Dependent)

The score can represent different semantics:

* **Leaderboard** → player score
* **Priority queue** → task priority
* **Time-series** → timestamp
* **Scheduling** → execution time

This makes Sorted Sets extremely flexible.

---

### 6. Lexicographical Ordering Feature

* When scores are equal, members are ordered alphabetically.
* Enables **string-range queries** when all scores are identical.
* Effectively creates an ordered index on strings.

---

## One-Line Summary

> Redis Sorted Sets store unique members ordered by a numeric score, offering fast ranking, range queries, and top-N operations, making them ideal for leaderboards, priority queues, and time-based data.



### When to Use Hashes vs Strings

The choice between Hashes and JSON strings depends on your access patterns. Use Hashes when you frequently access or update individual fields, when you have many objects with similar structures, or when memory efficiency matters. Use JSON strings when you always need the complete object, when objects have deeply nested structures, or when you're caching API responses that will be returned as-is.

As a rule of thumb, if you find yourself frequently parsing JSON to update a single field, you should probably use a Hash. If you're storing complete objects that are rarely modified, JSON strings might be simpler.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2005/images/2.svg)


### When to Use Sorted Sets vs Lists

Lists maintain insertion order, while Sorted Sets maintain score-based order. Use Lists when order is determined by time (first-in-first-out queues, recent items feeds) and elements don't need reordering. Use Sorted Sets when order is determined by a changing value (leaderboards, priority queues) or when you need range queries by score.

Lists provide O(1) operations at the ends but O(N) for accessing middle elements. Sorted Sets provide O(log N) for most operations, which is slower than O(1) but still fast (log N for a million items is only 20 operations).

## Lab Task Description

In this lab, you'll build four practical applications demonstrating Redis's Hash and Sorted Set data types:

1. **User Management with Hashes** - Rebuild the user system using Hashes for efficient field-level operations, atomic updates, and partial object retrieval.

2. **Real-time Leaderboard** - Create a leaderboard using Sorted Sets with instant updates. Implement "top 10 players," "player rank," and "players near me" features.

3. **Priority Task Queue** - Build a priority-based queue using Sorted Sets where high-priority tasks are processed first, unlike the FIFO queue from Lab 02.

4. **Time-Series Analytics** - Track website metrics over time using Sorted Sets, enabling efficient time-range queries for analytics.

You'll gain hands-on experience with these data structures, understanding their performance characteristics and when they provide advantages over simpler data types.

## Step-by-Step Solution

### Step 1: Project Setup and Redis Connection

Let's create a new project for this lab. If you still have Redis running from Lab 02, you can continue using it. Otherwise, start a Redis container:
```bash
docker run --name redis-lab3 -p 6379:6379 -d redis:latest
```

Create a new project directory:
```bash
mkdir redis-advanced-types-lab
cd redis-advanced-types-lab
```

Initialize the project and install dependencies:
```bash
npm init -y
npm install express ioredis
```

Create the project structure:
```bash
touch server.js redis.js
mkdir routes
touch routes/hashes.js routes/sortedsets.js
```

Create the Redis connection module. Open `redis.js`:
```javascript
const Redis = require('ioredis');

const redis = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
  maxRetriesPerRequest: 3
});

redis.on('connect', () => {
  console.log('âœ“ Redis client connected');
});

redis.on('ready', () => {
  console.log('âœ“ Redis client ready');
});

redis.on('error', (err) => {
  console.error('âœ— Redis error:', err.message);
});

process.on('SIGINT', async () => {
  console.log('\nShutting down gracefully...');
  await redis.quit();
  process.exit(0);
});

module.exports = redis;
```

Now create the main server. Open `server.js`:
```javascript
const express = require('express');
const redis = require('./redis');

const hashesRoutes = require('./routes/hashes');
const sortedSetsRoutes = require('./routes/sortedsets');

const app = express();
const PORT = 5000;

app.use(express.json());

// Mount routes
app.use('/api/hashes', hashesRoutes);
app.use('/api/sortedsets', sortedSetsRoutes);

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    redis: redis.status 
  });
});

app.listen(PORT, () => {
  console.log(`\nðŸš€ Server running on http://localhost:${PORT}`);
  console.log(`ðŸ“Š Health: http://localhost:${PORT}/health\n`);
});
```

This sets up our foundation with two route modules for Hashes and Sorted Sets.

| Goal                            | ioredis call                                   | Scope      |
| ------------------------------- | ---------------------------------------------- | ---------- |
| Check if key exists             | `redis.exists(userKey)`                        | Key        |
| Check if field exists           | `redis.hexists(userKey, 'email')`              | Hash field |
| Create / update field           | `redis.hset(userKey, 'field', value)`          | Hash field |
| Create / update multiple fields | `redis.hset(userKey, 'f1', v1, 'f2', v2)`      | Hash field |
| Read single field               | `redis.hget(userKey, 'field')`                 | Hash field |
| Read multiple fields            | `redis.hmget(userKey, 'field1', 'field2')`     | Hash field |
| Read entire hash                | `redis.hgetall(userKey)`                       | Hash       |
| Get number of fields            | `redis.hlen(userKey)`                          | Hash       |
| Get all field names             | `redis.hkeys(userKey)`                         | Hash       |
| Get all values                  | `redis.hvals(userKey)`                         | Hash       |
| Delete field(s)                 | `redis.hdel(userKey, 'field')`                 | Hash field |
| Delete entire hash              | `redis.del(userKey)`                           | Key        |
| Rename key                      | `redis.rename(userKey, newKey)`                | Key        |
| Set expiration (seconds)        | `redis.expire(userKey, 3600)`                  | Key        |
| Get TTL                         | `redis.ttl(userKey)`                           | Key        |
| Remove expiration               | `redis.persist(userKey)`                       | Key        |
| Increment integer field         | `redis.hincrby(userKey, 'count', 1)`           | Hash field |
| Increment float field           | `redis.hincrbyfloat(userKey, 'balance', 10.5)` | Hash field |
| Set field only if not exists    | `redis.hsetnx(userKey, 'email', 'test@x.com')` | Hash field |
| Get random field                | `redis.hrandfield(userKey)`                    | Hash       |
| Scan fields (cursor-based)      | `redis.hscan(userKey, cursor)`                 | Hash       |
| Delete key asynchronously       | `redis.unlink(userKey)`                        | Key        |



**`del, hdel, exists, hexists` returns 0 if not found or 1 when found and successful .**


### Step 2: Implementing Hash Operations for User Management

Let's implement a comprehensive user management system using Hashes. Open `routes/hashes.js`:
```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Create or update user using Hash
router.post('/users/:id', async (req, res) => {
  try {
    const userId = req.params.id;
    const { name, email, age, city } = req.body;

    if (!name || !email) {
      return res.status(400).json({ error: 'name and email required' });
    }

    const userKey = `user:${userId}`;

    // Store user fields in hash
    await redis.hset(userKey, {
      name,
      email,
      age: age || '',
      city: city || '',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    });

    res.status(201).json({
      message: 'User created',
      userId
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get complete user
router.get('/users/:id', async (req, res) => {
  try {
    const userId = req.params.id;
    const userKey = `user:${userId}`;

    // Get all fields and values from hash. hGetAll returns object format of the user.
    const user = await redis.hgetall(userKey);

    if (Object.keys(user).length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.json({
      userId,
      user
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get specific user fields
router.get('/users/:id/fields', async (req, res) => {
  try {
    const userId = req.params.id;
    const fields = req.query.fields; // e.g., ?fields=name,email

    if (!fields) {
      return res.status(400).json({ error: 'fields query parameter required' });
    }

    const userKey = `user:${userId}`;
    const fieldArray = fields.split(',');

    // Get multiple specific fields . It returns Array of values, in the same order as the requested fields
    const values = await redis.hmget(userKey, ...fieldArray);

    // Check if user exists
    if (values.every(v => v === null)) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Build response object
    const result = {};
    fieldArray.forEach((field, index) => {
      result[field] = values[index];
    });

    res.json({
      userId,
      fields: result
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update specific user field
router.patch('/users/:id/fields/:field', async (req, res) => {
  try {
    const userId = req.params.id;
    const field = req.params.field;
    const { value } = req.body;

    if (value === undefined) {
      return res.status(400).json({ error: 'value required' });
    }

    const userKey = `user:${userId}`;

    // Check if user exists
    const exists = await redis.exists(userKey);
    if (!exists) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Update single field
    await redis.hset(userKey, field, value);
    await redis.hset(userKey, 'updatedAt', new Date().toISOString());

    res.json({
      message: 'Field updated',
      userId,
      field,
      value
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Delete user field
router.delete('/users/:id/fields/:field', async (req, res) => {
  try {
    const userId = req.params.id;
    const field = req.params.field;
    const userKey = `user:${userId}`;

    // HDEL on a non-existent key Returns 0
    const deleted = await redis.hdel(userKey, field);

    if (!deleted) {
      return res.status(404).json({ error: 'Field not found' });
    }

    await redis.hset(userKey, 'updatedAt', new Date().toISOString());

    res.json({
      message: 'Field deleted',
      userId,
      field
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Check if field exists
router.get('/users/:id/fields/:field/exists', async (req, res) => {
  try {
    const userId = req.params.id;
    const field = req.params.field;
    const userKey = `user:${userId}`;

    const exists = await redis.hexists(userKey, field);

    res.json({
      userId,
      field,
      exists: exists === 1
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Increment user counter field
router.post('/users/:id/increment/:field', async (req, res) => {
  try {
    const userId = req.params.id;
    const field = req.params.field;
    const { amount } = req.body;

    const userKey = `user:${userId}`;

    // Atomically increment field
    const newValue = await redis.hincrby(userKey, field, amount || 1);

    res.json({
      message: 'Field incremented',
      userId,
      field,
      newValue
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Product inventory management using hashes
router.post('/products/:id', async (req, res) => {
  try {
    const productId = req.params.id;
    const { name, price, stock, category } = req.body;

    if (!name || price === undefined || stock === undefined) {
      return res.status(400).json({ error: 'name, price, and stock required' });
    }

    const productKey = `product:${productId}`;

    await redis.hset(productKey, {
      name,
      price: price.toString(),
      stock: stock.toString(),
      category: category || 'general',
      createdAt: new Date().toISOString()
    });

    res.status(201).json({
      message: 'Product created',
      productId
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get product
router.get('/products/:id', async (req, res) => {
  try {
    const productId = req.params.id;
    const productKey = `product:${productId}`;

    const product = await redis.hgetall(productKey);

    if (Object.keys(product).length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }

    // here product2 is a new object using product's key, value pairs . ...product is used to both create a new copy and allow rewrite for product2 .
    res.json({
      productId,
      product2: {
        ...product,
        price: parseFloat(product.price),
        stock: parseInt(product.stock)
      }
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update product stock (decrement for purchase)
router.post('/products/:id/purchase', async (req, res) => {
  try {
    const productId = req.params.id;
    const { quantity } = req.body;

    if (!quantity || quantity < 1) {
      return res.status(400).json({ error: 'valid quantity required' });
    }

    const productKey = `product:${productId}`;

    // Check if product exists
    const exists = await redis.exists(productKey);
    if (!exists) {
      return res.status(404).json({ error: 'Product not found' });
    }

    // Get current stock
    const currentStock = await redis.hget(productKey, 'stock');
    
    if (parseInt(currentStock) < quantity) {
      return res.status(400).json({ 
        error: 'Insufficient stock',
        available: parseInt(currentStock)
      });
    }

    // Atomically decrement stock
    const newStock = await redis.hincrby(productKey, 'stock', -quantity);

    res.json({
      message: 'Purchase successful',
      productId,
      quantity,
      remainingStock: newStock
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Session data with hash and expiration
router.post('/sessions', async (req, res) => {
  try {
    const { userId, metadata } = req.body;

    if (!userId) {
      return res.status(400).json({ error: 'userId required' });
    }

    const sessionId = `sess_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const sessionKey = `session:${sessionId}`;

    // Store session data in hash
    await redis.hset(sessionKey, {
      userId,
      ip: req.ip || 'unknown',
      userAgent: req.get('user-agent') || 'unknown',
      metadata: JSON.stringify(metadata || {}),
      createdAt: new Date().toISOString(),
      lastAccess: new Date().toISOString()
    });

    // Set expiration (30 minutes)
    await redis.expire(sessionKey, 1800);

    res.json({
      sessionId,
      expiresIn: 1800
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get session
router.get('/sessions/:id', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const sessionKey = `session:${sessionId}`;

    const session = await redis.hgetall(sessionKey);

    if (Object.keys(session).length === 0) {
      return res.status(404).json({ error: 'Session not found or expired' });
    }

    // Update last access time
    await redis.hset(sessionKey, 'lastAccess', new Date().toISOString());

    // Refresh expiration
    await redis.expire(sessionKey, 1800);

    const ttl = await redis.ttl(sessionKey);

    res.json({
      sessionId,
      session: {
        ...session,
        metadata: JSON.parse(session.metadata)
      },
      expiresIn: ttl
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```

This comprehensive module demonstrates the power of Hashes. The user management endpoints show how you can update individual fields without retrieving the entire object. The product inventory system uses atomic increments for stock management, preventing race conditions in concurrent purchases. The session system combines Hashes with expiration, showing how Hashes work seamlessly with TTL.

### Step 3: Implementing Sorted Set Operations for Leaderboards

#### Sorted Set (ZSET) operations

| Goal                         | ioredis call                                        | Scope      |
| ---------------------------- | --------------------------------------------------- | ---------- |
| Add / update member          | `redis.zadd(key, score, member)`                    | Sorted set |
| Add multiple members         | `redis.zadd(key, score1, member1, score2, member2)` | Sorted set |
| Get member score             | `redis.zscore(key, member)`                         | Sorted set |
| Get rank (low → high)        | `redis.zrank(key, member)`                          | Sorted set |
| Get rank (high → low)        | `redis.zrevrank(key, member)`                       | Sorted set |
| Get members by score         | `redis.zrangebyscore(key, min, max)`                | Sorted set |
| Get members by score (desc)  | `redis.zrevrangebyscore(key, max, min)`             | Sorted set |
| Get members by rank(asc)     | `redis.zrange(key, start, stop)`                    | Sorted set |
| Get members by rank(desc)    | `redis.zrevrange(key, start, stop)`                 | Sorted set |
| Get members + scores         | `redis.zrange(key, start, stop, 'WITHSCORES')`      | Sorted set |
| Count members in score range | `redis.zcount(key, min, max)`                       | Sorted set |
| Check if member exists       | `redis.zscore(key, member) !== null`                | Sorted set |
| Remove member(s)             | `redis.zrem(key, member)`                           | Sorted set |
| Remove by score range        | `redis.zremrangebyscore(key, min, max)`             | Sorted set |
| Remove by rank range         | `redis.zremrangebyrank(key, start, stop)`           | Sorted set |
| Get number of members        | `redis.zcard(key)`                                  | Sorted set |
| Increment member score       | `redis.zincrby(key, increment, member)`             | Sorted set |
| Scan sorted set              | `redis.zscan(key, cursor)`                          | Sorted set |
| Set expiration               | `redis.expire(key, seconds)`                        | Key        |
| Delete sorted set            | `redis.del(key)`                                    | Key        |



| Command    | Order               | Best for                           |
| ---------- | ------------------- | ---------------------------------- |
| `ZRANK`    | Lowest score first  | Time-based / penalty-based scoring |
| `ZREVRANK` | Highest score first | Traditional leaderboards           |


Now let's implement Sorted Set operations with a gaming leaderboard. Open `routes/sortedsets.js`:
```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Add or update player score
router.post('/leaderboard/players/:playerId/score', async (req, res) => {
  try {
    const playerId = req.params.playerId;
    const { score } = req.body;

    if (score === undefined) {
      return res.status(400).json({ error: 'score required' });
    }

    const leaderboardKey = 'leaderboard:global';

    // Add player with score (or update if exists)
    await redis.zadd(leaderboardKey, score, playerId);

    // Get player's rank (0-indexed, lower is better)
    const rank = await redis.zrevrank(leaderboardKey, playerId);

    res.json({
      message: 'Score updated',
      playerId,
      score,
      rank: rank + 1 // Convert to 1-indexed
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Increment player score
router.post('/leaderboard/players/:playerId/increment', async (req, res) => {
  try {
    const playerId = req.params.playerId;
    const { points } = req.body;

    if (points === undefined) {
      return res.status(400).json({ error: 'points required' });
    }

    const leaderboardKey = 'leaderboard:global';

    // Atomically increment score
    const newScore = await redis.zincrby(leaderboardKey, points, playerId);

    // Get updated rank
    const rank = await redis.zrevrank(leaderboardKey, playerId);

    res.json({
      message: 'Score incremented',
      playerId,
      points,
      newScore: parseFloat(newScore),
      rank: rank + 1
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get top N players
router.get('/leaderboard/top/:count', async (req, res) => {
  try {
    const count = parseInt(req.params.count) || 10;
    const leaderboardKey = 'leaderboard:global';

    // Get top players with scores (ZREVRANGE for descending order)
    const players = await redis.zrevrange(
      leaderboardKey, 
      0, 
      count - 1, 
      'WITHSCORES'
    );

    // return example 
    // [
    //   'playerA', '100',
    //   'playerB', '95',
    //   'playerC', '80'
    // ]

    // Format response
    const leaderboard = [];
    for (let i = 0; i < players.length; i += 2) {
      leaderboard.push({
        rank: (i / 2) + 1,
        playerId: players[i],
        score: parseFloat(players[i + 1])
      });
    }

    res.json({
      leaderboard,
      count: leaderboard.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get player rank and score
router.get('/leaderboard/players/:playerId', async (req, res) => {
  try {
    const playerId = req.params.playerId;
    const leaderboardKey = 'leaderboard:global';

    // Get score
    const score = await redis.zscore(leaderboardKey, playerId);

    if (score === null) {
      return res.status(404).json({ error: 'Player not found' });
    }

    // Get rank (0-indexed, descending order)
    const rank = await redis.zrevrank(leaderboardKey, playerId);

    // Get total players
    const totalPlayers = await redis.zcard(leaderboardKey);

    res.json({
      playerId,
      score: parseFloat(score),
      rank: rank + 1,
      totalPlayers
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get players around a specific player
router.get('/leaderboard/players/:playerId/nearby', async (req, res) => {
  try {
    const playerId = req.params.playerId;
    const range = parseInt(req.query.range) || 2;
    const leaderboardKey = 'leaderboard:global';

    // Get player's rank
    const rank = await redis.zrevrank(leaderboardKey, playerId);

    if (rank === null) {
      return res.status(404).json({ error: 'Player not found' });
    }

    // Get players around this rank
    const start = Math.max(0, rank - range);
    const end = rank + range;

    const players = await redis.zrevrange(
      leaderboardKey,
      start,
      end,
      'WITHSCORES'
    );

    // Format response
    const nearby = [];
    for (let i = 0; i < players.length; i += 2) {
      nearby.push({
        rank: start + (i / 2) + 1,
        playerId: players[i],
        score: parseFloat(players[i + 1]),
        isCurrentPlayer: players[i] === playerId
      });
    }

    res.json({
      playerId,
      nearby
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get players by score range
router.get('/leaderboard/range', async (req, res) => {
  try {
    const { min, max } = req.query;

    if (min === undefined || max === undefined) {
      return res.status(400).json({ error: 'min and max query parameters required' });
    }

    const leaderboardKey = 'leaderboard:global';

    // Get players with scores in range. ZRANGEBYSCORE returns all members whose score is within a given score range, ordered by score in ascending order by default.
    const players = await redis.zrangebyscore(
      leaderboardKey,
      parseFloat(min),
      parseFloat(max),
      'WITHSCORES'
    );

    // Format response
    const results = [];
    for (let i = 0; i < players.length; i += 2) {
      results.push({
        playerId: players[i],
        score: parseFloat(players[i + 1])
      });
    }

    res.json({
      min: parseFloat(min),
      max: parseFloat(max),
      players: results,
      count: results.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Remove player from leaderboard
router.delete('/leaderboard/players/:playerId', async (req, res) => {
  try {
    const playerId = req.params.playerId;
    const leaderboardKey = 'leaderboard:global';

    const removed = await redis.zrem(leaderboardKey, playerId);

    if (!removed) {
      return res.status(404).json({ error: 'Player not found' });
    }

    res.json({
      message: 'Player removed from leaderboard',
      playerId
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Priority task queue using sorted sets
router.post('/tasks', async (req, res) => {
  try {
    const { task, priority } = req.body;

    if (!task || priority === undefined) {
      return res.status(400).json({ error: 'task and priority required' });
    }

    const taskId = `task_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const queueKey = 'tasks:priority';

    const taskData = {
      id: taskId,
      task,
      createdAt: new Date().toISOString()
    };

    // Add task with priority as score (higher priority = higher score)
    await redis.zadd(queueKey, priority, JSON.stringify(taskData));

    // Get position in queue
    const rank = await redis.zrevrank(queueKey, JSON.stringify(taskData));

    res.status(201).json({
      message: 'Task added',
      taskId,
      priority,
      queuePosition: rank + 1
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get next high-priority task
router.get('/tasks/next', async (req, res) => {
  try {
    const queueKey = 'tasks:priority';

    // Get highest priority task (highest score)
    const tasks = await redis.zrevrange(queueKey, 0, 0, 'WITHSCORES');

    if (tasks.length === 0) {
      return res.json({
        message: 'Queue is empty',
        task: null
      });
    }

    const taskData = JSON.parse(tasks[0]);
    const priority = parseFloat(tasks[1]);

    // Remove task from queue
    await redis.zrem(queueKey, tasks[0]);

    res.json({
      message: 'Task retrieved',
      task: taskData,
      priority
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get queue status
router.get('/tasks/queue', async (req, res) => {
  try {
    const queueKey = 'tasks:priority';

    const queueSize = await redis.zcard(queueKey);

    // Get top 10 tasks
    const tasks = await redis.zrevrange(queueKey, 0, 9, 'WITHSCORES');

    const preview = [];
    for (let i = 0; i < tasks.length; i += 2) {
      preview.push({
        task: JSON.parse(tasks[i]),
        priority: parseFloat(tasks[i + 1])
      });
    }

    res.json({
      queueSize,
      preview
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Time-series data using sorted sets
router.post('/metrics/:metric', async (req, res) => {
  try {
    const metric = req.params.metric;
    const { value, timestamp } = req.body;

    if (value === undefined) {
      return res.status(400).json({ error: 'value required' });
    }

    const metricsKey = `metrics:${metric}`;
    const ts = timestamp || Date.now();

    // Use timestamp as score
    await redis.zadd(metricsKey, ts, `${ts}:${value}`);

    // Optional: Keep only last 1000 data points
    const count = await redis.zcard(metricsKey);
    if (count > 1000) {
      await redis.zremrangebyrank(metricsKey, 0, count - 1001);
    }

    res.json({
      message: 'Metric recorded',
      metric,
      value,
      timestamp: ts
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get metrics in time range
router.get('/metrics/:metric/range', async (req, res) => {
  try {
    const metric = req.params.metric;
    const { start, end } = req.query;

    if (!start || !end) {
      return res.status(400).json({ error: 'start and end timestamps required' });
    }

    const metricsKey = `metrics:${metric}`;

    // Get data points in time range
    const data = await redis.zrangebyscore(
      metricsKey,
      parseFloat(start),
      parseFloat(end)
    );

    // Parse data points. 
    // Score (timestamp)     Member
    // ----------------------------------------
    // 1705400000000         "1705400000000:42.3"
    // 1705400060000         "1705400060000:43.1"
    // 1705400120000         "1705400120000:41.9"
    // Not using WITHSCORES .So Redis returns only the sorted-set members, in ascending score (time) order.
    // [
    //   "1705400000000:42.3",
    //   "1705400060000:43.1",
    //   "1705400120000:41.9"
    // ]

    const points = data.map(point => {
      const [timestamp, value] = point.split(':');
      return {
        timestamp: parseInt(timestamp),
        value: parseFloat(value),
        date: new Date(parseInt(timestamp)).toISOString()
      };
    });

    res.json({
      metric,
      start: parseInt(start),
      end: parseInt(end),
      points,
      count: points.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get latest N metrics
router.get('/metrics/:metric/latest/:count', async (req, res) => {
  try {
    const metric = req.params.metric;
    const count = parseInt(req.params.count) || 10;

    const metricsKey = `metrics:${metric}`;

    // Get latest data points
    const data = await redis.zrevrange(metricsKey, 0, count - 1);

    // Parse data points
    const points = data.map(point => {
      const [timestamp, value] = point.split(':');
      return {
        timestamp: parseInt(timestamp),
        value: parseFloat(value),
        date: new Date(parseInt(timestamp)).toISOString()
      };
    });

    res.json({
      metric,
      points,
      count: points.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```

This module showcases Sorted Sets' versatility. The leaderboard system demonstrates real-time ranking with efficient updates and queries. The priority queue shows how scores can represent importance rather than timestamps. The time-series system uses timestamps as scores, enabling efficient time-range queries.

### Step 4: Testing Hash Operations

Start your server and test the Hash operations:
```bash
# Make sure Redis is running
docker start redis-lab3

# Start the server
node server.js
```

**Test user management with Hashes:**

Create a user:
```bash
curl -X POST http://localhost:5000/api/hashes/users/1 -H "Content-Type: application/json" -d '{"name": "Alice Johnson", "email": "alice@example.com", "age": 28, "city": "New York"}' | jq
```

Get the complete user:
```bash
curl http://localhost:5000/api/hashes/users/1 | jq
```

Get only specific fields:
```bash
curl "http://localhost:5000/api/hashes/users/1/fields?fields=name,email" | jq
```

This retrieves only the requested fields, not the entire object. Update a single field:
```bash
curl -X PATCH http://localhost:5000/api/hashes/users/1/fields/city -H "Content-Type: application/json" -d '{"value": "San Francisco"}' | jq
```

Only the city field is updatedâ€”no need to send the entire object. Check if a field exists:
```bash
curl http://localhost:5000/api/hashes/users/1/fields/age/exists | jq
```

Returns `"exists": true`. Check a non-existent field:
```bash
curl http://localhost:5000/api/hashes/users/1/fields/phone/exists | jq
```

Returns `"exists": false`.

**Test product inventory:**

Create a product:
```bash
curl -X POST http://localhost:5000/api/hashes/products/prod1 -H "Content-Type: application/json" -d '{"name": "Laptop", "price": 999.99, "stock": 50, "category": "electronics"}' | jq
```

Make a purchase (decrement stock):
```bash
curl -X POST http://localhost:5000/api/hashes/products/prod1/purchase -H "Content-Type: application/json" -d '{"quantity": 3}' | jq
```

The stock decreases by 3 atomically. Make multiple concurrent purchasesâ€”they all succeed without race conditions. Try to purchase more than available:
```bash
curl -X POST http://localhost:5000/api/hashes/products/prod1/purchase -H "Content-Type: application/json" -d '{"quantity": 100}' | jq
```

Returns "Insufficient stock" error.

**Test sessions with Hashes:**

Create a session:
```bash
curl -X POST http://localhost:5000/api/hashes/sessions -H "Content-Type: application/json" -d '{"userId": "user123", "metadata": {"theme": "dark", "language": "en"}}' | jq
```

Copy the sessionId from the response. Retrieve the session:
```bash
curl http://localhost:5000/api/hashes/sessions/YOUR_SESSION_ID | jq
```

Notice it includes all the hash fields. Each access refreshes the expiration. Wait 30 minutes and try againâ€”the session expires automatically.

### Step 5: Testing Sorted Set Operations

**Test the leaderboard:**

Add some players with scores:
```bash
curl -X POST http://localhost:5000/api/sortedsets/leaderboard/players/alice/score -H "Content-Type: application/json" -d '{"score": 1500}'

curl -X POST http://localhost:5000/api/sortedsets/leaderboard/players/bob/score -H "Content-Type: application/json" -d '{"score": 2000}'

curl -X POST http://localhost:5000/api/sortedsets/leaderboard/players/charlie/score -H "Content-Type: application/json" -d '{"score": 1800}'

curl -X POST http://localhost:5000/api/sortedsets/leaderboard/players/david/score -H "Content-Type: application/json" -d '{"score": 2200}'

curl -X POST http://localhost:5000/api/sortedsets/leaderboard/players/eve/score -H "Content-Type: application/json" -d '{"score": 1900}'
```

Get the top 3 players:
```bash
curl http://localhost:5000/api/sortedsets/leaderboard/top/3 | jq
```

You see david (2200), bob (2000), and eve (1900) in order. Increment a player's score:
```bash
curl -X POST http://localhost:5000/api/sortedsets/leaderboard/players/alice/increment -H "Content-Type: application/json" -d '{"points": 800}' | jq
```

Alice now has 2300 points and moves to rank 1. Check a player's rank:
```bash
curl http://localhost:5000/api/sortedsets/leaderboard/players/charlie | jq
```

Returns charlie's rank, score, and total players. Get players near charlie:
```bash
  curl "http://localhost:5000/api/sortedsets/leaderboard/players/charlie/nearby?range=2" | jq
```

Shows 2 players above and below charlie in the rankings. Query by score range:
```bash
curl "http://localhost:5000/api/sortedsets/leaderboard/range?min=1800&max=2000" | jq
```

Returns all players with scores between 1800 and 2000.

**Test priority queue:**

Add tasks with different priorities:
```bash
curl -X POST http://localhost:5000/api/sortedsets/tasks -H "Content-Type: application/json" -d '{"task": "Send email", "priority": 5}'

curl -X POST http://localhost:5000/api/sortedsets/tasks -H "Content-Type: application/json" -d '{"task": "Generate report", "priority": 8}'

curl -X POST http://localhost:5000/api/sortedsets/tasks -H "Content-Type: application/json" -d '{"task": "Update cache", "priority": 3}'

curl -X POST http://localhost:5000/api/sortedsets/tasks -H "Content-Type: application/json" -d '{"task": "Critical bug fix", "priority": 10}'
```

Check queue status:
```bash
curl http://localhost:5000/api/sortedsets/tasks/queue
```

Tasks are ordered by priority. Get the next task (highest priority):
```bash
curl http://localhost:5000/api/sortedsets/tasks/next
```

Returns "Critical bug fix" (priority 10). Get the next task again:
```bash
curl http://localhost:5000/api/sortedsets/tasks/next
```

Returns "Generate report" (priority 8). Tasks are processed by priority, not insertion order.

**Test time-series metrics:**

Record some metrics:
```bash
curl -X POST http://localhost:5000/api/sortedsets/metrics/cpu_usage -H "Content-Type: application/json" -d '{"value": 45.5, "timestamp": 1609459200000}'

curl -X POST http://localhost:5000/api/sortedsets/metrics/cpu_usage -H "Content-Type: application/json" -d '{"value": 52.3, "timestamp": 1609459260000}'

curl -X POST http://localhost:5000/api/sortedsets/metrics/cpu_usage -H "Content-Type: application/json" -d '{"value": 48.7, "timestamp": 1609459320000}'

curl -X POST http://localhost:5000/api/sortedsets/metrics/cpu_usage -H "Content-Type: application/json" -d '{"value": 61.2, "timestamp": 1609459380000}'
```

Get metrics in a time range:
```bash
curl "http://localhost:5000/api/sortedsets/metrics/cpu_usage/range?start=1609459200000&end=1609459400000"
```

Returns all metrics within that timestamp range. Get the latest 2 metrics:
```bash
curl http://localhost:5000/api/sortedsets/metrics/cpu_usage/latest/2
```

Returns the most recent data points.

### Step 6: Exploring Data in Redis CLI

Connect to Redis CLI to examine the data structures:
```bash
docker exec -it redis-lab3 redis-cli
```

**Examine Hash structure:**
```bash
HGETALL user:1
```

Shows all field-value pairs. Notice how readable it is compared to JSON strings. Get a single field:
```bash
HGET user:1 email
```

Check number of fields:
```bash
HLEN user:1
```

Get all field names:
```bash
HKEYS user:1
```

**Examine Sorted Set structure:**
```bash
ZRANGE leaderboard:global 0 -1 WITHSCORES
```

Shows all players with scores in ascending order. Get in descending order:
```bash
ZREVRANGE leaderboard:global 0 -1 WITHSCORES
```

Check number of members:
```bash
ZCARD leaderboard:global
```

Get a player's score:
```bash
ZSCORE leaderboard:global alice
```

Get a player's rank:
```bash
ZREVRANK leaderboard:global alice
```

Count members in score range:
```bash
ZCOUNT leaderboard:global 1800 2000
```

**Monitor operations:**
```bash
MONITOR
```

Make API requests in another terminal. You'll see HSET, HGET, ZADD, ZRANGE, and other commands. This helps understand how your application uses Redis.

### Step 7: Performance Comparison

Create a performance test script. Create `test-hash-performance.js`:
```javascript
const axios = require('axios');

async function compareHashVsString() {
  console.log('Testing Hash field update performance...\n');

  // Update single field with Hash (optimal)
  const start1 = Date.now();
  for (let i = 0; i < 100; i++) {
    await axios.patch(
      'http://localhost:5000/api/hashes/users/1/fields/age',
      { value: 28 + i }
    );
  }
  const hashTime = Date.now() - start1;

  console.log(`Hash: 100 field updates in ${hashTime}ms`);
  console.log(`Average: ${(hashTime / 100).toFixed(2)}ms per update\n`);

  console.log('Benefits of Hashes over JSON strings:');
  console.log('- No JSON parsing/serialization overhead');
  console.log('- No need to retrieve entire object');
  console.log('- Atomic field updates without race conditions');
  console.log('- More memory efficient for large objects');
}

compareHashVsString();
```

Run it:
```bash
node test-hash-performance.js
```

Now test Sorted Set performance. Create `test-sortedset-performance.js`:
```javascript
const axios = require('axios');

async function testSortedSetPerformance() {
  console.log('Adding 1000 players to leaderboard...\n');

  const start1 = Date.now();
  for (let i = 1; i <= 1000; i++) {
    await axios.post(
      `http://localhost:5000/api/sortedsets/leaderboard/players/player${i}/score`,
      { score: Math.floor(Math.random() * 10000) }
    );
  }
  const addTime = Date.now() - start1;

  console.log(`Added 1000 players in ${addTime}ms`);
  console.log(`Average: ${(addTime / 1000).toFixed(2)}ms per player\n`);

  // Test top 10 query
  const start2 = Date.now();
  await axios.get('http://localhost:5000/api/sortedsets/leaderboard/top/10');
  const queryTime = Date.now() - start2;

  console.log(`Retrieved top 10 from 1000 players in ${queryTime}ms`);
  console.log('\nSorted Set operations are O(log N), remaining fast even with millions of items');
}

testSortedSetPerformance();
```

Run it:
```bash
node test-sortedset-performance.js
```

Even with 1000 players, retrieving the top 10 takes just milliseconds. With traditional databases, you'd need indexes and ORDER BY queries that get slower as data grows.

### Step 8: Advanced Patterns

Let's implement some advanced patterns combining both data types. Add to `routes/hashes.js`:
```javascript
// User statistics using Hash with atomic increments
router.post('/stats/users/:userId/:action', async (req, res) => {
  try {
    const userId = req.params.userId;
    const action = req.params.action; // e.g., 'posts', 'likes', 'comments'

    const statsKey = `stats:user:${userId}`;

    // Atomically increment action counter
    const newCount = await redis.hincrby(statsKey, action, 1);

    // Also increment total actions
    await redis.hincrby(statsKey, 'totalActions', 1);

    // Get all stats
    const stats = await redis.hgetall(statsKey);

    res.json({
      userId,
      action,
      newCount,
      allStats: stats
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
```

Add to `routes/sortedsets.js`:
```javascript
// Daily active users tracking
router.post('/analytics/dau/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
    const dauKey = `dau:${today}`;

    // Add user to today's set with timestamp as score
    const timestamp = Date.now();
    await redis.zadd(dauKey, timestamp, userId);

    // Expire the key after 7 days
    await redis.expire(dauKey, 604800);

    // Get today's DAU count
    const count = await redis.zcard(dauKey);

    res.json({
      message: 'User activity recorded',
      userId,
      date: today,
      dauCount: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get DAU for a date
router.get('/analytics/dau/:date', async (req, res) => {
  try {
    const date = req.params.date;
    const dauKey = `dau:${date}`;

    const count = await redis.zcard(dauKey);
    const users = await redis.zrange(dauKey, 0, -1);

    res.json({
      date,
      dauCount: count,
      users
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
```

Test these advanced patterns:
```bash
# Track user actions
curl -X POST http://localhost:5000/api/hashes/stats/users/alice/posts | jq
curl -X POST http://localhost:5000/api/hashes/stats/users/alice/likes | jq
curl -X POST http://localhost:5000/api/hashes/stats/users/alice/likes | jq
curl -X POST http://localhost:5000/api/hashes/stats/users/alice/comments | jq

# Track daily active users
curl -X POST http://localhost:5000/api/sortedsets/analytics/dau/alice } | jq
curl -X POST http://localhost:5000/api/sortedsets/analytics/dau/bob | jq
curl -X POST http://localhost:5000/api/sortedsets/analytics/dau/charlie | jq

# Get today's DAU
curl http://localhost:5000/api/sortedsets/analytics/dau/<DATE>
```

Replace `<DATE>` with today's date in this `2025-11-02` format.

## Conclusion

In this lab, we've explored Redis's advanced data typesâ€”**Hashes** and **Sorted Sets**â€”building applications that demonstrate their power and efficiency.

**Hashes** proved superior to JSON strings for object storage, eliminating parsing overhead and enabling atomic field updates. We implemented efficient inventory management and session systems with field-level access, dramatically simplifying code and improving performance.

**Sorted Sets** demonstrated versatility across real-time leaderboards, priority queues, and time-series analytics. The automatic score-based ordering enables O(log N) operations that remain fast even with millions of members, eliminating application-level sorting.



# Specialized Data Types (Bitmaps, HyperLogLogs, Geo)

## Introduction

Redis's core data structures handle most use cases well, but at scale, some problems demand specialized solutions. Tracking millions of daily active users with Sets consumes massive memoryâ€”each user ID takes dozens of bytes. For 10 million users over 365 days, you're storing hundreds of gigabytes for boolean data. Counting unique visitors faces similar issuesâ€”storing millions of visitor IDs just to answer "how many?" Similarly, location-based features require complex distance calculations and spatial indexes that are inefficient to implement in application code.

Redis's specialized data types solve these problems elegantly. **Bitmaps** use one bit per data point (100-1000x memory reduction), **HyperLogLogs** count unique elements using fixed 12KB memory with ~1% error, and **Geospatial indexes** provide built-in distance calculations and radius searches.

In this lab, we'll build a user analytics system with Bitmaps, a unique visitor counter with HyperLogLogs, and a location-based service finder with Geo commandsâ€”demonstrating dramatic memory and performance improvements for specialized use cases.

## Understanding Specialized Data Types

### Bitmaps: Efficient Boolean Storage

Redis Bitmaps aren't actually a separate data typeâ€”they're a set of bit-oriented operations on Strings. However, conceptually, they provide a powerful way to store and manipulate arrays of bits. Each bit can be 0 or 1, representing boolean values like active/inactive, present/absent, or true/false.

The memory efficiency of Bitmaps is extraordinary. A traditional Set storing user IDs might use 20-50 bytes per user. A Bitmap uses exactly one bit per user, regardless of the user ID size. This means you can track 8 users in a single byte, or 8 million users in 1 megabyte. For boolean data across large populations, this represents a 100-1000x memory reduction.

Bitmaps support several powerful operations. You can set individual bits (SETBIT), retrieve bits (GETBIT), count set bits (BITCOUNT), and perform bitwise operations between bitmaps (BITOP). The BITOP command supports AND, OR, XOR, and NOT operations, enabling complex queries like "users active on both Monday AND Tuesday" or "users active on Monday OR Tuesday but NOT Wednesday."

The bit positions in a Bitmap can represent anythingâ€”user IDs, day numbers, feature flags, or permission bits. The key insight is mapping your domain (users, days, features) to bit positions. Once mapped, operations become extremely efficient. Checking if user 1000000 was active is O(1). Counting how many users were active is O(N) where N is the bitmap size in bytes, not the number of users.

Redis Bitmaps are sparseâ€”you don't allocate memory for unset bits. If you set bit 1000000, Redis only allocates enough memory to reach that position. The underlying String grows as needed. This sparsity makes Bitmaps practical even for very large bit positions.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2006/images/1.svg)

The main limitation of Bitmaps is that they work best for dense data or when you need bitwise operations. If you're tracking sparse data (like "users who purchased luxury yachts"), a Set might be more memory-efficient since it only stores present elements. Bitmaps shine when many bits are set, when you need bitwise operations, or when bit position is meaningful.

### HyperLogLogs: Probabilistic Cardinality Estimation

HyperLogLog is a probabilistic data structure that estimates the cardinality (count of unique elements) of a set. It uses fixed memoryâ€”about 12 kilobytesâ€”regardless of whether you're counting 100 unique elements or 100 billion, with a standard error of just 0.81%.

The algorithm works by hashing elements and examining their binary representation. The number of leading zeros provides statistical information about cardinality. Redis maintains 16,384 registers and uses harmonic mean to achieve accurate estimates. If the true count is 1,000,000, estimates typically fall between 991,900 and 1,008,100.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2006/images/3.svg)

Use HyperLogLog when you need to count unique elements and can tolerate ~1% error, when cardinality might be very large (millions or billions), or when memory is constrained. Don't use it when you need exact counts, need to retrieve actual elements, or when the number of unique elements is small (Sets work fine for thousands of elements).


HyperLogLog supports three main operations: PFADD to add elements, PFCOUNT to get the estimated count, and PFMERGE to combine multiple HyperLogLogs. The merge operation is particularly powerfulâ€”track unique visitors per hour, then merge all hourly HyperLogLogs to get unique daily visitors, enabling efficient aggregation across time periods or categories.

The memory savings are dramatic: counting 10 million unique visitors uses 12KB with HyperLogLog versus tens or hundreds of megabytes with a Set. Redis uses a sparse representation for small cardinalities, only transitioning to the full 12KB as cardinality grows.


### Geospatial Indexes: Location-Based Queries

Redis's Geospatial features provide a complete solution for location-based applications. They're implemented using Sorted Sets with geohash encodingâ€”geographical coordinates are encoded into a single 52-bit integer that preserves spatial locality, meaning nearby locations have similar scores.

Redis uses the WGS84 coordinate system (same as GPS) with standard longitude (-180 to 180) and latitude (-90 to 90). Locations are specified as longitude-latitude pairsâ€”note the order is opposite of common "lat-long" usage.

Key operations include: GEOADD (add locations), GEOPOS (retrieve coordinates), GEODIST (calculate distance using Haversine formula), GEORADIUS (find locations within radius of a point), and GEORADIUSBYMEMBER (find locations within radius of another location). Distance calculations account for Earth's curvature, providing accurate results without requiring geography knowledge.

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2006/images/4.svg)

Since Geospatial data uses Sorted Sets internally, you can use Sorted Set commands like ZREM, ZRANGE, and ZSCORE. Redis treats Earth as a perfect sphere with up to 0.5% error, which is excellent for finding nearby restaurants, map locations, or geofencing, though not suitable for sub-meter precision applications.

### When to Use These Specialized Types

The decision to use specialized types depends on your specific requirements. Use Bitmaps when you're tracking boolean states across many entities (millions of users, thousands of days), when you need bitwise operations (AND, OR, XOR), or when memory is critical. Use HyperLogLog when you need to count unique elements, the count might be very large, you can tolerate ~1% error, and you don't need the actual elements. Use Geospatial indexes when building location-based features, calculating distances, finding nearby locations, or implementing geofencing.

Don't use Bitmaps for sparse data, don't use HyperLogLog when you need exact counts or the actual elements, and don't use Geospatial indexes for non-location data. Understanding these tradeoffs ensures you choose the right tool for each problem.

## Lab Task Description

This hands-on lab demonstrates Redis's specialized data types through three real-world applications:

**1. User Analytics System (Bitmaps)**
- Track daily active users across millions of users with minimal memory
- Query patterns: "users active on both Monday AND Tuesday", "active this week but NOT last week"
- Calculate retention rates (Day 1 to Day 7)
- Achieve 100x+ memory reduction vs traditional Sets

**2. Unique Visitor Counter (HyperLogLogs)**
- Count unique visitors across pages, time periods, and campaigns
- Aggregate hourly â†’ daily â†’ monthly using merge operations
- Fixed 12KB memory regardless of scale
- Compare accuracy (~1% error) vs exact Set-based counting

**3. Location-Based Service Finder (Geospatial)**
- Store venues (restaurants, cafÃ©s) with GPS coordinates
- Implement "near me" searches with distance calculations
- Create geofencing alerts
- Use built-in geographic calculations (no complex geometry libraries needed)

## Step-by-Step Solution

### Step 1: Project Setup

Let's create a new project for this lab. Start Redis if it's not already running:
```bash
docker run --name redis-lab4 -p 6379:6379 -d redis:latest
```

Create the project structure:
```bash
mkdir redis-specialized-types-lab
cd redis-specialized-types-lab
npm init -y
npm install express ioredis
```

Create files:
```bash
touch server.js redis.js
mkdir routes
touch routes/bitmaps.js routes/hyperloglog.js routes/geo.js
```

Create the Redis connection module. Open `redis.js`:
```javascript
const Redis = require('ioredis');

const redis = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
  maxRetriesPerRequest: 3
});

redis.on('connect', () => {
  console.log('âœ“ Redis client connected');
});

redis.on('ready', () => {
  console.log('âœ“ Redis client ready');
});

redis.on('error', (err) => {
  console.error('âœ— Redis error:', err.message);
});

process.on('SIGINT', async () => {
  console.log('\nShutting down gracefully...');
  await redis.quit();
  process.exit(0);
});

module.exports = redis;
```

Create the main server. Open `server.js`:
```javascript
const express = require('express');
const redis = require('./redis');

const bitmapsRoutes = require('./routes/bitmaps');
const hyperloglogRoutes = require('./routes/hyperloglog');
const geoRoutes = require('./routes/geo');

const app = express();
const PORT = 5000;

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));


// Mount routes
app.use('/api/bitmaps', bitmapsRoutes);
app.use('/api/hll', hyperloglogRoutes);
app.use('/api/geo', geoRoutes);

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    redis: redis.status 
  });
});

app.listen(PORT, () => {
  console.log(`\n Server running on http://localhost:${PORT}`);
  console.log(` Health: http://localhost:${PORT}/health\n`);
});
```

### Step 2: Implementing Bitmap Operations for Analytics

Let's implement a comprehensive analytics system using Bitmaps. Open `routes/bitmaps.js`:
```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Helper function to get date string
function getDateString(date = new Date()) {
  return date.toISOString().split('T')[0];
}

// Track daily active user
router.post('/dau/:date/users/:userId', async (req, res) => {
  try {
    const date = req.params.date;
    const userId = parseInt(req.params.userId);

    if (isNaN(userId) || userId < 0) {
      return res.status(400).json({ error: 'userId must be non-negative integer' });
    }

    const dauKey = `dau:${date}`;

    // Set bit at position userId to 1
    await redis.setbit(dauKey, userId, 1);

    // Set expiration (keep data for 90 days)
    await redis.expire(dauKey, 7776000);

    // Get total active users for this day
    const count = await redis.bitcount(dauKey);

    res.json({
      message: 'User activity tracked',
      date,
      userId,
      totalActiveUsers: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Check if user was active on a date
router.get('/dau/:date/users/:userId', async (req, res) => {
  try {
    const date = req.params.date;
    const userId = parseInt(req.params.userId);

    const dauKey = `dau:${date}`;

    // Get bit at position userId
    const active = await redis.getbit(dauKey, userId);

    res.json({
      date,
      userId,
      active: active === 1
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get daily active user count
router.get('/dau/:date', async (req, res) => {
  try {
    const date = req.params.date;
    const dauKey = `dau:${date}`;

    const count = await redis.bitcount(dauKey);

    // Get memory usage
    const memory = await redis.call('MEMORY', 'USAGE', dauKey);

    res.json({
      date,
      activeUsers: count,
      memoryBytes: memory
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Users active on both date1 AND date2
router.get('/dau/intersection/:date1/:date2', async (req, res) => {
  try {
    const date1 = req.params.date1;
    const date2 = req.params.date2;

    const key1 = `dau:${date1}`;
    const key2 = `dau:${date2}`;
    const destKey = `dau:temp:${Date.now()}`;

    // Perform AND operation
    await redis.bitop('AND', destKey, key1, key2);

    // Count bits in result
    const count = await redis.bitcount(destKey);

    // Clean up temporary key
    await redis.del(destKey);

    res.json({
      date1,
      date2,
      operation: 'AND',
      activeOnBothDays: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Users active on date1 OR date2
router.get('/dau/union/:date1/:date2', async (req, res) => {
  try {
    const date1 = req.params.date1;
    const date2 = req.params.date2;

    const key1 = `dau:${date1}`;
    const key2 = `dau:${date2}`;
    const destKey = `dau:temp:${Date.now()}`;

    // Perform OR operation
    await redis.bitop('OR', destKey, key1, key2);

    const count = await redis.bitcount(destKey);

    await redis.del(destKey);

    res.json({
      date1,
      date2,
      operation: 'OR',
      activeOnEitherDay: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Users active on date1 but NOT date2
router.get('/dau/difference/:date1/:date2', async (req, res) => {
  try {
    const date1 = req.params.date1;
    const date2 = req.params.date2;

    const key1 = `dau:${date1}`;
    const key2 = `dau:${date2}`;
    const notKey2 = `dau:temp:not:${Date.now()}`;
    const destKey = `dau:temp:${Date.now()}`;

    // NOT date2
    await redis.bitop('NOT', notKey2, key2);

    // AND with date1
    await redis.bitop('AND', destKey, key1, notKey2);

    const count = await redis.bitcount(destKey);

    await redis.del(notKey2, destKey);

    res.json({
      date1,
      date2,
      operation: 'date1 AND NOT date2',
      activeOnDate1ButNotDate2: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Calculate retention rate
// retention refers to the proportion of a population, cohort, or sample that remains active, present, or in a specific state over a given period of time. It is generally used to measure stability, loyalty, or the success of a process in keeping subjects from "churning" (leaving or failing). 
router.get('/retention/:cohortDate/:returnDate', async (req, res) => {
  try {
    const cohortDate = req.params.cohortDate;
    const returnDate = req.params.returnDate;

    const cohortKey = `dau:${cohortDate}`;
    const returnKey = `dau:${returnDate}`;
    const destKey = `dau:temp:${Date.now()}`;

    // Get cohort size
    const cohortSize = await redis.bitcount(cohortKey);

    if (cohortSize === 0) {
      return res.json({
        cohortDate,
        returnDate,
        cohortSize: 0,
        retainedUsers: 0,
        retentionRate: 0
      });
    }

    // Users active on both dates
    await redis.bitop('AND', destKey, cohortKey, returnKey);
    const retainedUsers = await redis.bitcount(destKey);

    await redis.del(destKey);

    const retentionRate = (retainedUsers / cohortSize * 100).toFixed(2);

    res.json({
      cohortDate,
      returnDate,
      cohortSize,
      retainedUsers,
      retentionRate: `${retentionRate}%`
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Feature flags using bitmaps
router.post('/features/users/:userId', async (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    const { features } = req.body; // Array of feature names

    if (!Array.isArray(features)) {
      return res.status(400).json({ error: 'features array required' });
    }

    // Feature name to bit position mapping
    const featureMap = {
      'dark_mode': 0,
      'beta_features': 1,
      'premium': 2,
      'notifications': 3,
      'analytics': 4
    };

    const userKey = `user:${userId}:features`;

    // Set bits for enabled features
    for (const feature of features) {
      const bitPos = featureMap[feature];
      if (bitPos !== undefined) {
        await redis.setbit(userKey, bitPos, 1);
      }
    }

    res.json({
      message: 'Features enabled',
      userId,
      features
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Check if user has feature
router.get('/features/users/:userId/:feature', async (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    const feature = req.params.feature;

    const featureMap = {
      'dark_mode': 0,
      'beta_features': 1,
      'premium': 2,
      'notifications': 3,
      'analytics': 4
    };

    const bitPos = featureMap[feature];
    if (bitPos === undefined) {
      return res.status(400).json({ error: 'Unknown feature' });
    }

    const userKey = `user:${userId}:features`;

    const enabled = await redis.getbit(userKey, bitPos);

    res.json({
      userId,
      feature,
      enabled: enabled === 1
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get all features for user
router.get('/features/users/:userId', async (req, res) => {
  try {
    const userId = parseInt(req.params.userId);

    const featureMap = {
      'dark_mode': 0,
      'beta_features': 1,
      'premium': 2,
      'notifications': 3,
      'analytics': 4
    };

    const userKey = `user:${userId}:features`;

    const features = {};
    for (const [name, pos] of Object.entries(featureMap)) {
      const enabled = await redis.getbit(userKey, pos);
      features[name] = enabled === 1;
    }

    res.json({
      userId,
      features
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Bulk track multiple users (for testing)
router.post('/dau/:date/bulk', async (req, res) => {
  try {
    const date = req.params.date;
    const { userIds } = req.body;

    if (!Array.isArray(userIds)) {
      return res.status(400).json({ error: 'userIds array required' });
    }

    const dauKey = `dau:${date}`;

    // Set multiple bits
    const pipeline = redis.pipeline();
    for (const userId of userIds) {
      pipeline.setbit(dauKey, userId, 1);
    }
    await pipeline.exec();

    await redis.expire(dauKey, 7776000);

    const count = await redis.bitcount(dauKey);

    res.json({
      message: 'Bulk activity tracked',
      date,
      usersTracked: userIds.length,
      totalActiveUsers: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2006/images/7.svg)

This comprehensive module demonstrates Bitmaps' power for analytics. The DAU tracking uses one bit per user, enabling efficient memory usage even with millions of users. The bitwise operations (AND, OR, NOT) enable complex queries like retention analysis that would require multiple database queries with traditional storage.

### Step 3: Implementing HyperLogLog for Unique Counting

Now let's implement unique visitor counting with HyperLogLog. Open `routes/hyperloglog.js`:
```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Track unique visitor
router.post('/visitors/:page', async (req, res) => {
  try {
    const page = req.params.page;
    const { visitorId } = req.body;

    if (!visitorId) {
      return res.status(400).json({ error: 'visitorId required' });
    }

    const hllKey = `visitors:${page}`;

    // Add visitor to HyperLogLog
    await redis.pfadd(hllKey, visitorId);

    // Get estimated count
    const count = await redis.pfcount(hllKey);

    res.json({
      message: 'Visitor tracked',
      page,
      visitorId,
      uniqueVisitors: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get unique visitor count for page
router.get('/visitors/:page', async (req, res) => {
  try {
    const page = req.params.page;
    const hllKey = `visitors:${page}`;

    const count = await redis.pfcount(hllKey);

    // Get memory usage
    const memory = await redis.call('MEMORY', 'USAGE', hllKey);

    res.json({
      page,
      uniqueVisitors: count,
      memoryBytes: memory || 0
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Track hourly unique visitors
router.post('/hourly/:date/:hour', async (req, res) => {
  try {
    const date = req.params.date;
    const hour = req.params.hour;
    const { visitorId } = req.body;

    if (!visitorId) {
      return res.status(400).json({ error: 'visitorId required' });
    }

    const hour = req.params.hour.toString().padStart(2, '0');
    const hllKey = `hourly:${date}:${hour}`;


    await redis.pfadd(hllKey, visitorId);

    // Set expiration (keep hourly data for 7 days)
    await redis.expire(hllKey, 604800);

    const count = await redis.pfcount(hllKey);

    res.json({
      message: 'Hourly visitor tracked',
      date,
      hour,
      uniqueVisitors: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get daily unique visitors by merging hourly HLLs
router.get('/daily/:date', async (req, res) => {
  try {
    const date = req.params.date;

    // Create keys for all 24 hours
    const hourlyKeys = [];
    for (let hour = 0; hour < 24; hour++) {
      hourlyKeys.push(`hourly:${date}:${hour.toString().padStart(2, '0')}`);
    }

    const dailyKey = `daily:${date}:merged`;

    // Merge all hourly HLLs into daily HLL
    await redis.pfmerge(dailyKey, ...hourlyKeys);

    // Get count from merged HLL
    const count = await redis.pfcount(dailyKey);

    // Set expiration on merged result
    await redis.expire(dailyKey, 2592000); // 30 days

    res.json({
      date,
      uniqueVisitors: count,
      hoursAggregated: 24
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Track campaign visitors
router.post('/campaigns/:campaignId', async (req, res) => {
  try {
    const campaignId = req.params.campaignId;
    const { visitorId } = req.body;

    if (!visitorId) {
      return res.status(400).json({ error: 'visitorId required' });
    }

    const hllKey = `campaign:${campaignId}`;

    await redis.pfadd(hllKey, visitorId);

    const count = await redis.pfcount(hllKey);

    res.json({
      message: 'Campaign visitor tracked',
      campaignId,
      uniqueVisitors: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Compare unique visitors across multiple campaigns
router.get('/campaigns/compare', async (req, res) => {
  try {
    const { campaigns } = req.query; // Comma-separated campaign IDs

    if (!campaigns) {
      return res.status(400).json({ error: 'campaigns query parameter required' });
    }

    const campaignIds = campaigns.split(',');
    const results = [];

    for (const campaignId of campaignIds) {
      const hllKey = `campaign:${campaignId}`;
      const count = await redis.pfcount(hllKey);
      results.push({
        campaignId,
        uniqueVisitors: count
      });
    }

    res.json({
      campaigns: results
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get unique visitors across all campaigns (union)
router.get('/campaigns/total', async (req, res) => {
  try {
    const { campaigns } = req.query;

    if (!campaigns) {
      return res.status(400).json({ error: 'campaigns query parameter required' });
    }

    const campaignIds = campaigns.split(',');
    const campaignKeys = campaignIds.map(id => `campaign:${id}`);
    const mergedKey = `campaigns:merged:${Date.now()}`;

    // Merge all campaign HLLs
    await redis.pfmerge(mergedKey, ...campaignKeys);

    const count = await redis.pfcount(mergedKey);

    // Clean up temporary key
    await redis.del(mergedKey);

    res.json({
      campaigns: campaignIds,
      totalUniqueVisitors: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Bulk add visitors (for testing)
router.post('/visitors/:page/bulk', async (req, res) => {
  try {
    const page = req.params.page;
    const { visitorIds } = req.body;

    if (!Array.isArray(visitorIds)) {
      return res.status(400).json({ error: 'visitorIds array required' });
    }

    const hllKey = `visitors:${page}`;

    // Add multiple visitors at once
    await redis.pfadd(hllKey, ...visitorIds);

    const count = await redis.pfcount(hllKey);

    res.json({
      message: 'Bulk visitors tracked',
      page,
      visitorsAdded: visitorIds.length,
      uniqueVisitors: count
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Demonstrate HyperLogLog accuracy (both num and str accepted as hyperloglog hashes it)
router.post('/accuracy-test', async (req, res) => {
  try {
    const { count } = req.body;
    const testCount = count || 10000;

    const hllKey = 'test:accuracy';
    const setKey = 'test:accuracy:set';

    // Clear any existing test data
    await redis.del(hllKey, setKey);

    // Add same elements to both HLL and Set
    const visitors = [];
    for (let i = 0; i < testCount; i++) {
      visitors.push(`visitor_${i}`);
    }

    // Add to HLL
    await redis.pfadd(hllKey, ...visitors);

    // Add to Set (for exact count comparison)
    await redis.sadd(setKey, ...visitors);

    // Get counts
    const hllCount = await redis.pfcount(hllKey);
    const exactCount = await redis.scard(setKey);

    // Get memory usage
    const hllMemory = await redis.call('MEMORY', 'USAGE', hllKey);
    const setMemory = await redis.call('MEMORY', 'USAGE', setKey);

    const error = Math.abs(hllCount - exactCount);
    const errorPercent = ((error / exactCount) * 100).toFixed(4);

    // Clean up
    await redis.del(hllKey, setKey);

    res.json({
      uniqueElements: exactCount,
      hllEstimate: hllCount,
      error,
      errorPercent: `${errorPercent}%`,
      hllMemoryBytes: hllMemory,
      setMemoryBytes: setMemory,
      memorySavings: `${((1 - hllMemory / setMemory) * 100).toFixed(2)}%`
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```

This module demonstrates HyperLogLog's power for cardinality estimation. The visitor tracking uses fixed memory regardless of visitor count. The merge operations enable aggregating across time periods or categories efficiently. The accuracy test shows the tradeoff between memory savings and precision.

### Step 4: Implementing Geospatial Operations

Now let's implement location-based features with Geo commands. Open `routes/geo.js`:
```javascript
const express = require('express');
const router = express.Router();
const redis = require('../redis');

// Add location
router.post('/locations/:category', async (req, res) => {
  try {
    const category = req.params.category;
    const { name, longitude, latitude } = req.body;

    if (!name || longitude === undefined || latitude === undefined) {
      return res.status(400).json({ 
        error: 'name, longitude, and latitude required' 
      });
    }

    // Validate coordinates
    if (longitude < -180 || longitude > 180) {
      return res.status(400).json({ 
        error: 'longitude must be between -180 and 180' 
      });
    }

    if (latitude < -90 || latitude > 90) {
      return res.status(400).json({ 
        error: 'latitude must be between -90 and 90' 
      });
    }

    const geoKey = `locations:${category}`;

    // Add location with coordinates
    // Note: GEOADD takes longitude first, then latitude
    await redis.geoadd(geoKey, longitude, latitude, name);

    res.status(201).json({
      message: 'Location added',
      category,
      name,
      coordinates: {
        longitude,
        latitude
      }
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get location coordinates
router.get('/locations/:category/:name', async (req, res) => {
  try {
    const category = req.params.category;
    const name = req.params.name;

    const geoKey = `locations:${category}`;

    // Get coordinates for location
    // Case A — Location exists
    // [
    //   ["-73.985428", "40.748817"]
    // ]

    // That means:

    // result[0] === ["-73.985428", "40.748817"];

    // Case B — Location does not exist
    // [
    //   null
    // ]

    const result = await redis.geopos(geoKey, name);

    if (!result[0]) {
      return res.status(404).json({ error: 'Location not found' });
    }

    const [longitude, latitude] = result[0];

    res.json({
      category,
      name,
      coordinates: {
        longitude: parseFloat(longitude),
        latitude: parseFloat(latitude)
      }
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Calculate distance between two locations
router.get('/distance/:category/:name1/:name2', async (req, res) => {
  try {
    const category = req.params.category;
    const name1 = req.params.name1;
    const name2 = req.params.name2;
    const unit = req.query.unit || 'km'; // m, km, mi, ft

    const geoKey = `locations:${category}`;

    // Calculate distance
    const distance = await redis.geodist(geoKey, name1, name2, unit);

    if (distance === null) {
      return res.status(404).json({ 
        error: 'One or both locations not found' 
      });
    }

    res.json({
      category,
      from: name1,
      to: name2,
      distance: parseFloat(distance),
      unit
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Find locations within radius of coordinates
router.get('/nearby/:category', async (req, res) => {
  try {
    const category = req.params.category;
    const { longitude, latitude, radius, unit } = req.query;

    if (!longitude || !latitude || !radius) {
      return res.status(400).json({ 
        error: 'longitude, latitude, and radius query parameters required' 
      });
    }

    const geoKey = `locations:${category}`;
    const searchUnit = unit || 'km';

    // Find locations within radius
    // WITHDIST returns distances, WITHCOORD returns coordinates
    const results = await redis.georadius(
      geoKey,
      parseFloat(longitude),
      parseFloat(latitude),
      parseFloat(radius),
      searchUnit,
      'WITHDIST',
      'WITHCOORD',
      'ASC' // Sort by distance ascending
    );

    // Format results
    const locations = results.map(result => ({
      name: result[0],
      distance: parseFloat(result[1]),
      coordinates: {
        longitude: parseFloat(result[2][0]),
        latitude: parseFloat(result[2][1])
      }
    }));

    res.json({
      category,
      searchCenter: {
        longitude: parseFloat(longitude),
        latitude: parseFloat(latitude)
      },
      radius: parseFloat(radius),
      unit: searchUnit,
      locations,
      count: locations.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Find locations within radius of another location
router.get('/nearby/:category/:name', async (req, res) => {
  try {
    const category = req.params.category;
    const name = req.params.name;
    const { radius, unit, count } = req.query;

    if (!radius) {
      return res.status(400).json({ 
        error: 'radius query parameter required' 
      });
    }

    const geoKey = `locations:${category}`;
    const searchUnit = unit || 'km';
    const maxCount = count ? parseInt(count) : undefined;

    // Find locations within radius of a member
    const args = [
      geoKey,
      name,
      parseFloat(radius),
      searchUnit,
      'WITHDIST',
      'WITHCOORD',
      'ASC'
    ];

    if (maxCount) {
      args.push('COUNT', maxCount);
    }

    const results = await redis.georadiusbymember(...args);

    const locations = results.map(result => ({
      name: result[0],
      distance: parseFloat(result[1]),
      coordinates: {
        longitude: parseFloat(result[2][0]),
        latitude: parseFloat(result[2][1])
      }
    }));

    res.json({
      category,
      centerLocation: name,
      radius: parseFloat(radius),
      unit: searchUnit,
      locations,
      count: locations.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Remove location
router.delete('/locations/:category/:name', async (req, res) => {
  try {
    const category = req.params.category;
    const name = req.params.name;

    const geoKey = `locations:${category}`;

    // Geo data is stored as Sorted Set, use ZREM
    const removed = await redis.zrem(geoKey, name);

    if (!removed) {
      return res.status(404).json({ error: 'Location not found' });
    }

    res.json({
      message: 'Location removed',
      category,
      name
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get all locations in category
router.get('/locations/:category', async (req, res) => {
  try {
    const category = req.params.category;
    const geoKey = `locations:${category}`;

    // Get all members (Geo is a Sorted Set)
    const names = await redis.zrange(geoKey, 0, -1);

    if (names.length === 0) {
      return res.json({
        category,
        locations: [],
        count: 0
      });
    }

    // Get coordinates for all locations
    const coordinates = await redis.geopos(geoKey, ...names);

    const locations = names.map((name, index) => ({
      name,
      coordinates: coordinates[index] ? {
        longitude: parseFloat(coordinates[index][0]),
        latitude: parseFloat(coordinates[index][1])
      } : null
    }));

    res.json({
      category,
      locations,
      count: locations.length
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Bulk add locations (for testing)
router.post('/locations/:category/bulk', async (req, res) => {
  try {
    const category = req.params.category;
    const { locations } = req.body;

    if (!Array.isArray(locations)) {
      return res.status(400).json({ error: 'locations array required' });
    }

    const geoKey = `locations:${category}`;

    // Prepare arguments for GEOADD
    const args = [geoKey];
    for (const loc of locations) {
      args.push(loc.longitude, loc.latitude, loc.name);
    }

    const added = await redis.geoadd(...args);

    res.json({
      message: 'Locations added',
      category,
      added
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Geofencing: Check if location is within area
router.post('/geofence/check', async (req, res) => {
  try {
    const { 
      longitude, 
      latitude, 
      fenceLongitude, 
      fenceLatitude, 
      radius, 
      unit 
    } = req.body;

    if (!longitude || !latitude || !fenceLongitude || !fenceLatitude || !radius) {
      return res.status(400).json({ 
        error: 'longitude, latitude, fenceLongitude, fenceLatitude, and radius required' 
      });
    }

    const geoKey = 'geofence:temp';
    const fenceUnit = unit || 'km';

    // Add fence center temporarily
    await redis.geoadd(geoKey, fenceLongitude, fenceLatitude, 'fence_center');

    // Add location to check
    await redis.geoadd(geoKey, longitude, latitude, 'check_point');

    // Calculate distance
    const distance = await redis.geodist(geoKey, 'fence_center', 'check_point', fenceUnit);

    // Clean up
    await redis.del(geoKey);

    const inside = parseFloat(distance) <= parseFloat(radius);

    res.json({
      checkPoint: { longitude, latitude },
      fenceCenter: { longitude: fenceLongitude, latitude: fenceLatitude },
      radius: parseFloat(radius),
      unit: fenceUnit,
      distance: parseFloat(distance),
      inside
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
```


![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2006/images/6.svg)


This module demonstrates Geospatial operations' power for location-based features. The radius searches enable "near me" functionality. The distance calculations use accurate geographic formulas. The geofencing shows how to implement location-based triggers.

### Step 5: Testing Bitmap Operations

Start your server and test the Bitmap operations:
```bash
# Make sure Redis is running
docker start redis-lab4

# Start the server
node server.js
```

**Test daily active users:**

Track some users for today:
```bash
curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-01/users/1 | jq

curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-01/users/5 | jq

curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-01/users/10 | jq

curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-01/users/100 | jq

curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-01/users/1000 | jq
```

Get DAU count:
```bash
curl http://localhost:5000/api/bitmaps/dau/2025-11-01 | jq
```

Notice the memory usageâ€”probably just a few hundred bytes for 5 users. With traditional Sets, this would be much more. Track users for another day:
```bash
curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-02/users/1  | jq

curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-02/users/10 | jq

curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-02/users/50 | jq

curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-02/users/100 | jq
```

Find users active on both days (intersection):
```bash
curl http://localhost:5000/api/bitmaps/dau/intersection/2025-11-01/2025-11-02 | jq
```

Returns users 1, 10, and 100. Find users active on either day (union):
```bash
curl http://localhost:5000/api/bitmaps/dau/union/2025-11-01/2025-11-02| jq
```

Returns all unique users across both days. Calculate retention:
```bash
curl http://localhost:5000/api/bitmaps/retention/2025-11-01/2025-11-02| jq
```

Shows what percentage of Day 1 users returned on Day 2.

**Test bulk operations:**

Add many users at once:
```bash
curl -X POST http://localhost:5000/api/bitmaps/dau/2025-11-03/bulk -H "Content-Type: application/json" -d '{"userIds": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]}'| jq
```

**Test feature flags:**

Enable features for a user:
```bash
curl -X POST http://localhost:5000/api/bitmaps/features/users/1 -H "Content-Type: application/json" -d '{"features": ["dark_mode", "premium", "notifications"]}'| jq
```

Check if user has a specific feature:
```bash
curl http://localhost:5000/api/bitmaps/features/users/1/premium| jq
```

Returns `"enabled": true`. Get all features for user:
```bash
curl http://localhost:5000/api/bitmaps/features/users/1| jq
```

### Step 6: Testing HyperLogLog Operations

**Test unique visitors:**

Track visitors to different pages:
```bash
curl -X POST http://localhost:5000/api/hll/visitors/homepage -H "Content-Type: application/json" -d '{"visitorId": "visitor_1"}' | jq

curl -X POST http://localhost:5000/api/hll/visitors/homepage -H "Content-Type: application/json" -d '{"visitorId": "visitor_2"}' | jq

curl -X POST http://localhost:5000/api/hll/visitors/homepage -H "Content-Type: application/json" -d '{"visitorId": "visitor_1"}' | jq
```

Note: visitor_1 appears twice, but HLL only counts unique. Get unique visitor count:
```bash
curl http://localhost:5000/api/hll/visitors/homepage | jq
```

Returns 2 unique visitors, and shows memory usage.

**Test bulk operations:**

Add many visitors at once:
```bash
# Generate array of visitor IDs
node -e "console.log(JSON.stringify({visitorIds: Array.from({length:10000}, (_, i) => 'visitor_' + i)}))" > visitors.json

curl -X POST http://localhost:5000/api/hll/visitors/homepage/bulk -H "Content-Type: application/json" -d @visitors.json
```

Check countâ€”should be around 10,000 (with small error). Memory usage remains fixed around 12KB.

**Test accuracy:**

Run the accuracy test:
```bash
curl -X POST http://localhost:5000/api/hll/accuracy-test -H "Content-Type: application/json" -d '{"count": 100000}'
```

This compares HyperLogLog against an exact Set. You'll see error is typically less than 1%, but memory savings are 95%+ for large sets.

**Test hourly aggregation:**

Track hourly visitors:
```bash
curl -X POST http://localhost:5000/api/hll/hourly/2025-11-01/09 -H "Content-Type: application/json" -d '{"visitorId": "user_1"}' | jq

curl -X POST http://localhost:5000/api/hll/hourly/2025-11-01/09 -H "Content-Type: application/json" -d '{"visitorId": "user_2"}' | jq

curl -X POST http://localhost:5000/api/hll/hourly/2025-11-01/10 -H "Content-Type: application/json" -d '{"visitorId": "user_1"}' | jq

curl -X POST http://localhost:5000/api/hll/hourly/2025-11-01/10 -H "Content-Type: application/json" -d '{"visitorId": "user_3"}' | jq
```

Aggregate to daily:
```bash
curl http://localhost:5000/api/hll/daily/2025-11-01 | jq
```

Returns 3 unique visitors across all hours (user_1, user_2, user_3).

**Test campaigns:**

Track campaign visitors:
```bash
curl -X POST http://localhost:5000/api/hll/campaigns/email -H "Content-Type: application/json" -d '{"visitorId": "user_1"}' | jq

curl -X POST http://localhost:5000/api/hll/campaigns/email -H "Content-Type: application/json" -d '{"visitorId": "user_2"}' | jq

curl -X POST http://localhost:5000/api/hll/campaigns/social -H "Content-Type: application/json" -d '{"visitorId": "user_2"}' | jq

curl -X POST http://localhost:5000/api/hll/campaigns/social -H "Content-Type: application/json" -d '{"visitorId": "user_3"}' | jq
```

Compare campaigns:
```bash
curl "http://localhost:5000/api/hll/campaigns/compare?campaigns=email,social"curl -X POST http://localhost:5000/api/hll/campaigns/email -H "Content-Type: application/json" -d '{"visitorId": "user_1"}' | jq

curl -X POST http://localhost:5000/api/hll/campaigns/email -H "Content-Type: application/json" -d '{"visitorId": "user_2"}' | jq

curl -X POST http://localhost:5000/api/hll/campaigns/social -H "Content-Type: application/json" -d '{"visitorId": "user_2"}' | jq

curl -X POST http://localhost:5000/api/hll/campaigns/social -H "Content-Type: application/json" -d '{"visitorId": "user_3"}' | jq
```

Get total unique visitors across campaigns:
```bash
curl "http://localhost:5000/api/hll/campaigns/total?campaigns=email,social" | jq
```

Returns 3 (user_1, user_2, user_3 combined).

### Step 7: Testing Geospatial Operations

**Add sample locations:**

Add some restaurants in San Francisco:
```bash
curl -X POST http://localhost:5000/api/geo/locations/restaurants -H "Content-Type: application/json" -d '{"name": "Golden Gate Diner", "longitude": -122.4194, "latitude": 37.7749}' | jq

curl -X POST http://localhost:5000/api/geo/locations/restaurants -H "Content-Type: application/json" -d '{"name": "Bay Area Bistro", "longitude": -122.4089, "latitude": 37.7833}' | jq

curl -X POST http://localhost:5000/api/geo/locations/restaurants -H "Content-Type: application/json" -d '{"name": "Pacific Grill", "longitude": -122.4194, "latitude": 37.8080}' | jq

curl -X POST http://localhost:5000/api/geo/locations/restaurants -H "Content-Type: application/json" -d '{"name": "Mission Cafe", "longitude": -122.4194, "latitude": 37.7599}' | jq
```

Get a location's coordinates:
```bash
curl http://localhost:5000/api/geo/locations/restaurants/Golden%20Gate%20Diner | jq
```

**Calculate distances:**

Find distance between two restaurants:
```bash
curl "http://localhost:5000/api/geo/distance/restaurants/Golden%20Gate%20Diner/Bay%20Area%20Bistro?unit=km" | jq
```

Try different units:
```bash
curl "http://localhost:5000/api/geo/distance/restaurants/Golden%20Gate%20Diner/Bay%20Area%20Bistro?unit=mi" | jq
```

**Find nearby locations:**

Find restaurants within 2km of coordinates:
```bash
curl "http://localhost:5000/api/geo/nearby/restaurants?longitude=-122.4194&latitude=37.7749&radius=2&unit=km" | jq 
```

Returns restaurants sorted by distance. Find restaurants near another restaurant:
```bash
curl "http://localhost:5000/api/geo/nearby/restaurants/Golden%20Gate%20Diner?radius=5&unit=km" | jq
```

Limit results:
```bash
curl "http://localhost:5000/api/geo/nearby/restaurants/Golden%20Gate%20Diner?radius=10&unit=km&count=2" | jq
```

**Test geofencing:**

Check if a point is inside a geofence:
```bash
curl -X POST http://localhost:5000/api/geo/geofence/check -H "Content-Type: application/json" -d '{"longitude": -122.4200, "latitude": 37.7750, "fenceLongitude": -122.4194, "fenceLatitude": 37.7749, "radius": 1, "unit": "km"}' | jq
```

Returns `"inside": true` if within radius.

**Bulk add locations:**
```bash
curl -X POST http://localhost:5000/api/geo/locations/coffee/bulk -H "Content-Type: application/json" -d '{"locations": [{"name": "Blue Bottle", "longitude": -122.4092, "latitude": 37.7850}, {"name": "Ritual Coffee", "longitude": -122.4114, "latitude": 37.7615}, {"name": "Sightglass", "longitude": -122.4138, "latitude": 37.7764}]}' | jq
```

List all locations in category:
```bash
curl http://localhost:5000/api/geo/locations/coffee | jq
```

## Conclusion

In this lab, we explored Redis's specialized data types through practical applications. Bitmaps provided 100x memory reduction for boolean tracking with bitwise operations. HyperLogLogs delivered fixed 12KB memory cardinality estimation with under 1% error. Geospatial indexes enabled location-based features with built-in distance calculations. Use Bitmaps for dense boolean data, HyperLogLogs for approximate counting of large sets, and Geospatial for location queriesâ€”each offering dramatic memory or performance improvements for specific use cases. In the next lab, we'll explore Redis transactions, Lua scripting, and atomic operations.


Here is the fully rewritten, clean, and structured interview-style answer on **Consistent Hashing evolution**, using **exactly 4 virtual nodes per physical server** for the final example (small number chosen for clarity — real systems use 50–500+).

### 1. Starting Point: Naive Modulo-Based Sharding
We initially used a simple hash function:  
`server_index = hash(key) % num_servers`

**Problem**: Adding or removing even one server forces massive data movement.  
Example: 3 servers → add 4th server  
- Keys stay on the same server only if `hash(key) % 3 == hash(key) % 4`  
- Probability a key stays = **1/(n+1)** = **25%** (for n=3)  
→ **~75% of all data must be moved/reshuffled** — very expensive and slow.

### 2. Better Approach: Basic Consistent Hashing Ring (Fixed Size, e.g., 0–99)
We switched to a **ring** (circular hash space):  
- Fixed virtual ring of size **100** (positions 0 to 99)  
- Each physical server is placed at one position: `hash(server_name) % 100`  
- For a key: compute `pos = hash(key) % 100`, then find the **first server clockwise** (≥ pos, wrap around)

**Textual Ring Example** (3 servers placed at positions 20, 50, 80):

```
      0 ───────────── 99/0
     /                  \
   90                    10
  /                        \
80   ← redis-2             20   ← redis-0
  \                        /
   70                    30
     \                  /
      60 ───── 50 ───── 40
                ↑
             redis-1
```

**Improvement**: When adding a new server (e.g., redis-3 at 65), **only the keys between the previous owner and the new position move** → typically **~1/(n+1)** fraction of data moves (much better than 75%).

**Still Problems**:
- Uneven load: one server might own a large arc (e.g., 40% of the ring) → hotspots  
- **Cascading failure risk**: If redis-1 dies, its entire large arc suddenly goes to the next server → that server overloads → chain reaction

### 3. Final & Best Solution: Consistent Hashing with Virtual Nodes
To fix imbalance and cascading issues, we introduce **multiple virtual nodes** (replicas) per physical server, **randomly scattered** around the ring.

**How it works**:
1. For each physical server, create **k virtual nodes** (e.g., k=4)  
   Virtual node position = `hash(physical_server_name + "-" + i) % 100`  (i = 0 to 3)
2. Place all virtual nodes on the ring
3. Each ring position belongs to the **nearest clockwise virtual node**
4. That virtual node maps back to its **physical server**

**Example with 3 physical servers, 4 virtual nodes each** (12 virtual nodes total on ring 0–99):

| Virtual Node          | Hash Position (example) | Physical Server |
|-----------------------|--------------------------|-----------------|
| redis-0-v0            | 7                        | redis-0         |
| redis-0-v1            | 41                       | redis-0         |
| redis-0-v2            | 84                       | redis-0         |
| redis-0-v3            | 97                       | redis-0         |
| redis-1-v0            | 14                       | redis-1         |
| redis-1-v1            | 35                       | redis-1         |
| redis-1-v2            | 56                       | redis-1         |
| redis-1-v3            | 78                       | redis-1         |
| redis-2-v0            | 2                        | redis-2         |
| redis-2-v1            | 28                       | redis-2         |
| redis-2-v2            | 49                       | redis-2         |
| redis-2-v3            | 71                       | redis-2         |

→ Each physical server owns ~33 positions, but **scattered** → no large consecutive blocks.

**Simplified Ring View** (positions with owners):

```
                          0       2(redis-2)          
        97(redis-0)                           7(redis-0)     
                                         
                                                  14(redis-1)

                                                             28(redis-2)                             
       84(redis-0)                                                       
                                                             35(redis-1)

       78(redis-1)                                      41(redis-0)           
                                         
            71(redis-2)                           49(redis-2)
                          56(redis-1)
                                        
                                                  
                                                           
                                                                      
```

**Key Advantages**:
- **Excellent load balance**: With enough virtual nodes, each physical server gets almost equal share (variance drops dramatically)
- **No cascading failure**: If redis-1 dies, its 4 virtual nodes disappear → its ~33 positions are taken by **different neighboring virtual nodes** → load spreads across **all remaining servers**
- **Smooth scaling**: Adding redis-3 → create 4 new virtual nodes for it → only those ~4% of positions move → very little data reshuffling
- **Minimal disruption** on failure/add/remove

### Summary Table: Evolution of Approaches

| Approach                     | Data Movement on Add/Remove | Load Balance | Failure Isolation | Use Case Fit                  |
|------------------------------|------------------------------|--------------|-------------------|-------------------------------|
| Simple hash % n              | ~75% (n=3→4)                | Good         | Good              | Tiny/static clusters          |
| Basic ring (1 node/server)   | ~25% (1/(n+1))              | Poor         | Poor              | Small clusters, no hotspots   |
| Ring + Virtual Nodes (k=4+)  | ~1/(n+1) or better          | Excellent    | Excellent         | Production (Redis, Cassandra, Dynamo) |

This is why **consistent hashing with virtual nodes** became the standard in distributed caches (Redis Cluster), NoSQL databases (Cassandra, DynamoDB), and many load balancers.

(Real systems often use 100–1000 virtual nodes per server for near-perfect balance — here we used 4 just to keep the example readable.)
