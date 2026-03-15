# Replication Logic (Step 2)

## Replication Metadata

The system creates a metadata object describing the replication result.

```js
let replicationInfo = {
  mode: effectiveMode,
  replicas: 0,
  status: 'async'
};
```

Example output:

```json
{
  "mode": "sync",
  "replicas": 1,
  "status": "confirmed"
}
```

### Fields

| Field      | Description                                                   |
| ---------- | ------------------------------------------------------------- |
| `mode`     | Replication mode used (`sync` or `async`)                     |
| `replicas` | Number of replicas that confirmed the write                   |
| `status`   | Replication result (`async`, `confirmed`, `timeout`, `error`) |

---

# Replication Modes

The system supports **two replication modes**.

## 1. Async Replication (Default)

The primary node writes the data and **returns success immediately**. Replication happens in the background.

```
Client → Primary → OK
                ↘ Replica later
```

### Advantages

* Fast
* Low latency

### Tradeoff

* Replica might not have the data immediately.

Example result:

```json
{
  "mode": "async",
  "replicas": 0,
  "status": "async"
}
```

---

## 2. Sync Replication

The primary waits until **at least one replica confirms receiving the write**.

```
Client → Primary → Replica → confirm → OK
```

### Advantages

* Stronger consistency
* Lower risk of data loss

### Tradeoff

* Slower
* Can timeout if replicas do not respond

Example result:

```json
{
  "mode": "sync",
  "replicas": 1,
  "status": "confirmed"
}
```

---

# Redis WAIT Command

For synchronous replication the system uses the Redis `WAIT` command.

```js
const replicated = await node.primary.client.wait(1, 1000);
```

Equivalent Redis CLI command:

```
WAIT 1 1000
```

### Parameters

| Parameter | Meaning                                |
| --------- | -------------------------------------- |
| `1`       | Wait for **1 replica acknowledgement** |
| `1000`    | Wait up to **1000 milliseconds**       |

The command returns:

```
number_of_replicas_that_confirmed
```

### Example Return Values

| Returned | Meaning                             |
| -------- | ----------------------------------- |
| `1`      | One replica confirmed               |
| `2`      | Two replicas confirmed              |
| `0`      | No replica confirmed before timeout |

---

# Handling Replication Results

```js
replicationInfo.replicas = replicated;
replicationInfo.status = replicated >= 1 ? 'confirmed' : 'timeout';
```

### Success Case

```
replicated = 1
```

```json
{
  "replicas": 1,
  "status": "confirmed"
}
```

### Timeout Case

```
replicated = 0
```

```json
{
  "replicas": 0,
  "status": "timeout"
}
```

Warning logged:

```
[ReplicationRing] Sync replication timeout for key: <key>
```

Meaning the **primary wrote successfully but the replica did not confirm within the timeout window**.

---

# Error Handling

If Redis throws an error (network issue, node failure, etc.), the system records it:

```js
catch (error) {
  replicationInfo.status = 'error';
  replicationInfo.error = error.message;
}
```

Example:

```json
{
  "status": "error",
  "error": "connection lost"
}
```

---

# How Redis Replication Works

In the configuration:

```yaml
--replicaof cache_node_1_primary 6379
```

The replica opens a **persistent TCP replication stream** with the primary.

Replication flow:

```
Client
  ↓
Primary receives SET
  ↓
Primary writes to memory
  ↓
Primary streams command to replica
  ↓
Replica applies command
```

Important points:

* Replication is **stream-based**, not polling.
* Commands are **propagated directly** to replicas.

Example command propagation:

```
SET user:1 "Alice"
```

Primary sends the same command to replica:

```
SET user:1 "Alice"
```

---

# What WAIT Actually Does

`WAIT` **does not trigger replication**.

Replication already happens automatically.

`WAIT` only waits until the replica **acknowledges the write**.

Sequence:

```
Client
  ↓
SET key value
  ↓
Primary writes locally
  ↓
Primary streams command to replica
  ↓
Replica applies command
  ↓
Replica sends ACK
  ↓
WAIT completes
```

---

# Replication Latency

Typical replication times:

| Environment           | Latency   |
| --------------------- | --------- |
| Same machine (Docker) | < 1 ms    |
| Same datacenter       | 1–5 ms    |
| Cross region          | 50–200 ms |

Since containers run in the same Docker network:

```yaml
networks:
  - cache-network
```

Replication latency is typically **< 5 ms**.

Therefore:

```
WAIT 1 1000
```

provides a **very large safety margin**.

---

# Stronger Durability

If the cluster has multiple replicas, the system can require more confirmations.

Example:

```
WAIT 2 1000
```

Meaning:

* Wait until **2 replicas confirm the write**.

This approach is used by distributed systems like:

* Redis Cluster
* Cassandra
* DynamoDB

### Typical Usage

| Mode  | When Used                            |
| ----- | ------------------------------------ |
| Async | Performance and latency are critical |
| Sync  | Data durability is more important    |

---

# Startup Consideration

In Docker environments:

```yaml
depends_on:
  redis-node-1-primary:
    condition: service_healthy
```

Replicas start **after the primary**, but the **initial full sync** may take time during container startup.

This delay happens **only during initialization**, not during normal operation.

---

# 1️⃣ Does both Primary and Replica have `INFO replication`?

✅ **Yes.**
Both nodes support:

```bash
INFO replication
```

But the output depends on the role.

### Primary (Master) example

```
role:master
connected_slaves:1
master_repl_offset:1258723
repl_backlog_active:1
```

### Replica example

```
role:slave
master_host:127.0.0.1
master_port:6379
master_link_status:up
slave_repl_offset:1258723
```

So:

| Node Type | Important Fields                          |
| --------- | ----------------------------------------- |
| Primary   | `connected_slaves`, `master_repl_offset`  |
| Replica   | `master_link_status`, `slave_repl_offset` |

---

# 2️⃣ What is `master_repl_offset`?

`master_repl_offset` is the **replication position of the master**.

Think of it like a **replication log index**.

Every time Redis processes a write:

```
SET key value
```

Redis increments the **replication offset**.

Example timeline:

| Operation | Offset |
| --------- | ------ |
| Start     | 100    |
| SET A     | 120    |
| SET B     | 140    |
| SET C     | 160    |

If the replica processed everything, its offset will also be **160**.

---

# 3️⃣ How replication lag is calculated

Lag is basically:

```
master_offset - replica_offset
```

Example:

```
Master offset = 160
Replica offset = 150
```

Lag:

```
160 - 150 = 10
```

Meaning the replica is **10 operations behind**.


