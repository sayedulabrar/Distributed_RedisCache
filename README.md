# Handling Node Failures & Automatic Recovery

In the previous lab, we built a fault-tolerant system with replicationâ€”our data is safely backed up on replica nodes. But there's a critical problem: **when a primary fails, the service goes down for 33% of total keys!**

In this lab, we'll transform our distributed cache from **fault-tolerant** to **highly available** by implementing automatic failure detection and recovery. We'll build a self-healing system that keeps serving requests even when Redis nodes crash.

## The Problem We're Solving

### Current State

You have replication, but it's passive. The replica is sitting there with all the data, but your coordinator doesn't know to use it!

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/Distributed_cachel601.drawio.svg)

**The replica is sitting there with all the data, but your coordinator doesn't know to use it!**

### What we'll Build

A **self-healing distributed cache** that:

1. **Detects failures automatically** (within 15 seconds)
2. **Routes reads to replicas** when primaries fail
3. **Promotes replicas to primaries** for write capability
4. **Handles recovery** when failed primaries come back
5. **Maintains 100% availability** for reads during failures

## Architecture Overview

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/Distributed_cachel602.drawio.svg)

## Environment Setup

Clone this repository and checkout Lab 6:

```bash
git clone https://github.com/poridhioss/Distributed_RedisCache.git
git checkout lab/06
cd Distributed_RedisCache
```

## Codebase Walkthrough

### Component 1: Health Monitor (`HealthMonitor.js`)

**Objectives:**
- Continuously monitor the health of all Redis primary nodes
- Detect node failures through periodic health checks
- Track node status history and failure counts
- Trigger failover procedures when failure threshold is exceeded
- Handle primary node recovery scenarios

**Design Approach:**

The Health Monitor operates as a background service that implements a **heartbeat mechanism**:

- **Periodic Health Checks:** Every 5 seconds, ping all primary Redis nodes to verify availability
- **Failure Threshold Logic:** Uses a consecutive failure counter (default: 3 failures) to distinguish between transient network issues and genuine node failures
- **Status Tracking:** Maintains detailed state for each node including:
  - Current status (HEALTHY, FAILED, FAILED_OVER, RECOVERED)
  - Failure count and timestamps
  - Last successful health check
  - Uptime calculations

- **Event Logging:** Records all health state transitions for observability and debugging
- **Smart Recovery Detection:** When a previously failed primary comes back online, coordinates with the Failover Manager to properly reconfigure the topology

### Component 2: Failover Manager (`FailoverManager.js`)

**Objectives:**
- Execute automatic failover to replica when primary fails
- Promote replicas to writable primary nodes
- Route write operations to the correct node during and after failover
- Manage primary recovery and topology reconfiguration
- Track failover metrics and performance

**Primary Recovery Strategy:**

When a failed primary recovers:
- **New Topology Preservation:** Keep the promoted replica as the new primary
- **Role Reversal:** Configure the recovered (old) primary as a replica of the promoted node
- **Replication Setup:** Use `REPLICAOF` command to establish replication from recovered node to promoted primary
- **Consistency Guarantee:** Ensure read-only mode on the new replica

**Write Routing Intelligence:**

- Provides `getWriteTarget()` method that always returns the current active primary
- Handles the complexity of determining write destination after failover
- Prevents writes during the promotion window to ensure consistency

## Understanding Failover Behavior

### What Happens During Failover

**Timeline:**

**1. **T+0s:** Primary node crashes**

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-16.png)

**2. **T+5s:** First health check fails, failure count = 1**

**3. **T+10s:** Second health check fails, failure count = 2**

**4. **T+15s:** Third health check fails, failure count = 3 â†’ **Failover Triggered****

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-17.png)

**5. **T+15.5s:** Replica promoted to primary (typically < 500ms)**

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-18.png)

**6. **T+16s:** New primary accepting reads and writes**

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-19.png)

**Data Consistency:**
- **Async Replication:** May lose writes from last few seconds before failure
- **Sync Replication:** No data loss, replica is guaranteed to have all data
- **After Promotion:** All new writes go to promoted replica, no data loss

**TIMELINE OF DATA STATES** 

1. Node failure

    ![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-27.png)

2. Node Recovery

    ![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-28.png)

### What Happens During Recovery

**When Failed Primary Restarts:**

1. Health monitor detects primary is responsive again

    ![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-20.png)

2. Failover manager evaluates current topology
3. **Decision:** Keep promoted replica as primary (preserves new writes)
4. **Action:** Reconfigure old primary as replica of promoted node
5. **Result:** 
   - Promoted replica remains primary
   - Old primary catches up via replication
   - System reaches eventual consistency

    ![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-22.png)

### Component 3: Enhanced Hash Ring (`ConsistentHashRingWithReplication.js`)

**Objectives:**

- Integrate health monitoring and failover capabilities into the hash ring
- Implement automatic read failover to replicas
- Support writes to promoted replicas
- Maintain data consistency during failover transitions
- Provide comprehensive system observability

**Initialization:**
- Creates instances of `HealthMonitor` and `FailoverManager` during construction
- Starts automatic health monitoring after all Redis connections are established
- Ensures graceful shutdown that stops monitoring before disconnecting

**Read Operations with Failover:**
- **Primary-First Strategy:** Always attempt reads from current primary first
- **Automatic Replica Fallback:** On primary failure, transparently switch to replica
- **Response Metadata:** Include source information (primary/replica) and failover indicator
- **Dual-Failure Handling:** Return appropriate error when both primary and replica are unavailable

**Write Operations with Failover Awareness:**

- **Promotion Detection:** Check if node is currently in failover state
- **Write Target Resolution:** Use `FailoverManager.getWriteTarget()` to determine correct destination
- **Temporary Unavailability:** Block writes during promotion window with helpful error messages
- **Post-Failover Writes:** Automatically route to promoted replica once promotion completes
- **Replication Mode Support:** Maintain sync/async replication semantics even after failover

**State Management:**
- Track which nodes have undergone failover
- Maintain correct primary/replica references after promotion
- Update internal mappings to reflect new topology

## Starting the System

Build and start all services:

```bash
docker compose up -d --build
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image.png)

Monitor coordinator initialization:

```bash
docker compose logs -f coordinator
```

Expected log output indicates:
- Successful Redis connections (primary and replica)
- Health monitoring activation
- Failover manager initialization
- Background health checks starting


![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-1.png)


You can check other containers logs to see if everything is working or not.

## Testing Scenarios

### Test 1: Verify Health Monitoring

**Objective:** Confirm all nodes are being monitored and report healthy status

```bash
curl http://localhost:3001/health/nodes | jq
```

**Expected Response:**
Each node should show:
- `status: "HEALTHY"`
- Recent `lastCheck` timestamp
- `failCount: 0`
- Uptime since last successful check

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-2.png)

### Test 2: Load Data

**Objective:** Populate the cache to prepare for failover testing

```bash
chmod +x populate-cache.sh
./populate-cache.sh
```

Verify even distribution:

```bash
curl http://localhost:3001/distribution | jq
```

**What to Check:**
- Keys distributed across all 3 nodes
- Each node has ~33% of total keys
- Replica keys match primary keys (replication working)

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-3.png)

### Test 3: Simulate Primary Node Failure

**Objective:** Demonstrate automatic failover and read availability during primary failure

**Step 1:** Write a test key and identify which node stores it

```bash
curl -X POST http://localhost/cache \
  -H "Content-Type: application/json" \
  -d '{"key":"test:replication","value":"value"}' | jq
```

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-4.png)

> Note which `cache_node_X` stores this key from the response.

**Step 2:** Check initial system health

```bash
curl -s http://localhost:3001/health/summary | jq
```

Expected: `overallHealth: "healthy"`, all nodes healthy.

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-8.png)


**Step 3:** Verify key is readable from primary

```bash
curl -s http://localhost/cache/test:replication | jq
```

Expected: `source: "primary"`, value returned successfully

![alt text](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-12.png)

**Step 4:** Simulate primary failure by stopping the container

```bash
# Replace X with your node number
docker stop cache_node_X_primary
```

**Step 5:** Wait 15 seconds for failure detection (3 Ã— 5-second health checks)

Monitor the coordinator logs to observe:
1. First health check failure logged
2. Second health check failure logged
3. Third health check failure â†’ threshold exceeded
4. Failover triggered automatically
5. Replica promotion completed
6. Node marked as `FAILED_OVER`

**Step 6:** Verify automatic failover occurred

```bash
curl -s http://localhost:3001/health/nodes | jq '.cache_node_X'
```

Expected:
- `status: "FAILED_OVER"`
- `promoted: true`

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-13.png)

**Step 7:** Access the same key (should automatically read from replica)

```bash
curl -s http://localhost/cache/test:replication | jq
```

Expected:
- `success: true`
- `source: "replica"` or `source: "primary"` (promoted replica)
- `failover: true` (indicating failover occurred)
- Value returned successfully (no data loss!)

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-14.png)

**Step 8:** Check failover metrics

```bash
curl -s http://localhost:3001/failover/status | jq
```

Expected:
- `totalFailovers: 1`
- `successfulFailovers: 1`
- `averageFailoverTime` (typically < 1000ms)
- Node listed in `nodeStatus` with promotion details

![](https://raw.githubusercontent.com/poridhiEng/lab-asset/fe5ef59db451240fd91d7b1b24933bb06615a879/Redis%20Distributed%20Cache/Lab%2006/images/image-15.png)

**Step 9:** Restart the failed primary

```bash
docker start cache_node_X_primary
```

Wait 5-10 seconds for recovery detection.

**Step 10:** Verify recovery and new topology

```bash
curl -s http://localhost:3001/health/nodes | jq '.cache_node_X'
```

Expected:
- Old primary now configured as replica
- Promoted replica remains as primary
- Replication link re-established

## Conclusion

In this lab, we transformed our distributed cache from merely fault-tolerant into a highly available system by adding automatic failure detection through configurable health checks, seamless read failover that transparently redirects requests to replicas when primaries fail, and automatic promotion that allows replicas to become writable primaries without manual intervention. We also implemented smart recovery mechanisms to intelligently manage topology changes when failed nodes return, along with complete observability via rich metrics and APIs for monitoring system health. The result is a self-healing cache that maintains 100% read availability and near-zero write downtime during node failures, allowing applications to remain completely unaware of failures as the coordinator handles everything automatically.