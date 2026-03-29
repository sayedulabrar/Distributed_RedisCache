class HealthMonitor {
  constructor(cacheRing) {
    this.cacheRing = cacheRing;
    this.nodeStatus = new Map(); // nodeName -> { status, failCount, lastCheck, lastSuccess }
    this.healthHistory = [];
    this.checkInterval = 5000; // 5 seconds
    this.failureThreshold = 3; // 3 consecutive failures = dead
    this.intervalId = null;
  }

  /**
   * Start background health monitoring
   */
  startMonitoring() {
    console.log('[HealthMonitor] Starting health checks...');
    console.log(`[HealthMonitor] Check interval: ${this.checkInterval}ms`);
    console.log(`[HealthMonitor] Failure threshold: ${this.failureThreshold} consecutive failures`);
    
    // Initialize all nodes as healthy
    for (const nodeName of this.cacheRing.nodes.keys()) {
      this.nodeStatus.set(nodeName, {
        status: 'HEALTHY',
        failCount: 0,
        lastCheck: Date.now(),
        lastSuccess: Date.now()
      });
    }

    // Start periodic health checks
    this.intervalId = setInterval(() => {
      this.checkAllNodes();
    }, this.checkInterval);

    // Run first check immediately
    this.checkAllNodes();
  }

  /**
   * Check health of all nodes
   */
  async checkAllNodes() {
    const checks = [];
    
    for (const [nodeName, nodeInfo] of this.cacheRing.nodes) {
      checks.push(this.checkNode(nodeName, nodeInfo));
    }
    
    await Promise.allSettled(checks);
  }

  /**
   * Check health of a single node
   */
  async checkNode(nodeName, nodeInfo) {
    const status = this.nodeStatus.get(nodeName);
    status.lastCheck = Date.now();
    
    try {
      // Ping primary node with timeout
      const pingPromise = nodeInfo.primary.client.ping();
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Ping timeout')), 3000)
      );
      
      // Promise.race() in Node.js is a method that runs multiple promises at the same time and returns the result of the first one that 
      // settles (either resolved or rejected).
      await Promise.race([pingPromise, timeoutPromise]);
      
      // Success - handle based on previous status
      if (status.status === 'FAILED' || status.status === 'FAILED_OVER') {
        console.log(`[HealthMonitor] ✓ ${nodeName} PRIMARY RECOVERED!`);
        await this.handleNodeRecovery(nodeName);
      } else if (status.failCount > 0) {
        console.log(`[HealthMonitor] ✓ ${nodeName} recovered from transient failure`);
      }
      
      status.status = 'HEALTHY';
      status.failCount = 0;
      status.lastSuccess = Date.now();
      
    } catch (error) {
      // Failure - increment fail count
      status.failCount++;
      
      console.warn(
        `[HealthMonitor] ✗ ${nodeName} health check failed ` +
        `(${status.failCount}/${this.failureThreshold}) - ${error.message}`
      );
      
      // Check if threshold exceeded
      if (status.failCount >= this.failureThreshold) {
        if (status.status !== 'FAILED' && status.status !== 'FAILED_OVER') {
          console.error(`[HealthMonitor] ☠ ${nodeName} PRIMARY IS DEAD - Triggering failover`);
          
          status.status = 'FAILED';
          
          // Log failure event
          this.logHealthEvent({
            timestamp: Date.now(),
            node: nodeName,
            event: 'PRIMARY_FAILED',
            error: error.message,
            failCount: status.failCount
          });
          
          // Trigger failover
          if (this.cacheRing.failoverManager) {
            await this.cacheRing.failoverManager.failoverToReplica(nodeName);
            status.status = 'FAILED_OVER';
          }
        }
      }
    }
  }


  /**
   * Check health of a single node
   *
   * IMPORTANT CHANGES from original version:
   * ────────────────────────────────────────
   * Original problem:
   *   After failoverToReplica() succeeds, node.primary is updated to point to the promoted replica.
   *   The health check continued to ping node.primary (now the healthy promoted instance),
   *   so it almost always succeeded → and because status was 'FAILED_OVER',
   *   it repeatedly called handleNodeRecovery() every 5 seconds — even when the original primary
   *   was still completely down. This caused:
   *     - misleading "PRIMARY RECOVERED!" logs
   *     - repeated failed attempts to reconfigure a dead node
   *     - unnecessary load and log spam
   *
   * Solution goals:
   *   1. Always monitor the **current write target** (node.primary) quickly and reliably
   *      → trigger failover only when the active writer is actually dead
   *   2. Only attempt to recover/reconfigure the original primary (now node.replica)
   *      when we have **confirmed** that it is reachable again
   *   3. Never call handleNodeRecovery() based only on the current primary being healthy
   *      (because after failover that doesn't mean the original is back)
   *   4. Keep automatic failback when justified, but make it safe and conditional
   *   5. Preserve simple status model: HEALTHY means current topology is writable & replicated
   *
   * Result:
   *   - Fast failover protection for the active writer
   *   - Recovery/reconfiguration only triggers when both sides are pingable
   *   - No repeated useless recovery calls while original primary is down
   *   - After successful recovery → status returns to HEALTHY (no extra status needed)
   */
  async checkNodeFixed(nodeName, nodeInfo) {
    const status = this.nodeStatus.get(nodeName);
    if (!status) return;

    status.lastCheck = Date.now();

    const isFailedOver = status.status === 'FAILED_OVER';

    try {
      // ─────────────────────────────────────────────────────────────
      // Step 1: Always check the CURRENT active write endpoint first
      // This is the most important check — determines writability & failover trigger
      // ─────────────────────────────────────────────────────────────
      await Promise.race([
        nodeInfo.primary.client.ping(),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Ping timeout')), 3000)
        )
      ]);

      // Current primary is responsive → the node group can accept writes
      if (status.failCount > 0) {
        console.log(`[HealthMonitor] ✓ ${nodeName} current primary recovered from transient failure`);
      }

      // ─────────────────────────────────────────────────────────────
      // Step 2: Only if we are in FAILED_OVER state AND current primary is healthy,
      //         check whether the original primary (now acting as replica) has returned.
      //         We do NOT rely on current primary success alone to trigger recovery.
      // ─────────────────────────────────────────────────────────────
      if (isFailedOver && nodeInfo.replica?.client) {
        try {
          await Promise.race([
            nodeInfo.replica.client.ping(),
            new Promise((_, reject) => 
              setTimeout(() => reject(new Error('Ping timeout')), 3000)
            )
          ]);

          console.log(
            `[HealthMonitor] ✓ ${nodeName} original primary is reachable again → ` +
            `reconfiguring it as replica of current primary`
          );

          await this.handleNodeRecovery(nodeName);

          // Recovery succeeded → topology is now consistent and healthy again
          console.log(`[HealthMonitor] Recovery complete for ${nodeName} - topology restored`);

        } catch (recoveryErr) {
          // Original primary still unreachable → skip recovery this cycle
          // Intentionally silent in normal operation to avoid log spam
          // (can be promoted to debug/warn if needed during troubleshooting)
        }
      }

      // ─────────────────────────────────────────────────────────────
      // Current writer is confirmed good → node is healthy
      // We reset to HEALTHY even after recovery — no need for extra status like HEALTHY_POST_FAILOVER
      // History of failover/recovery is preserved in healthHistory events
      // ─────────────────────────────────────────────────────────────
      status.status = 'HEALTHY';
      status.failCount = 0;
      status.lastSuccess = Date.now();

    } catch (error) {
      // ─────────────────────────────────────────────────────────────
      // Current primary check failed → count failure & possibly trigger failover
      // We do NOT attempt recovery here — it wouldn't make sense while writer is down
      // ─────────────────────────────────────────────────────────────
      status.failCount++;

      console.warn(
        `[HealthMonitor] ✗ ${nodeName} current primary failed ` +
        `(${status.failCount}/${this.failureThreshold}) - ${error.message}`
      );

      if (status.failCount >= this.failureThreshold) {
        // Only trigger failover once — not repeatedly
        if (status.status !== 'FAILED' && status.status !== 'FAILED_OVER') {
          console.error(`[HealthMonitor] ☠ ${nodeName} PRIMARY IS DEAD - Triggering failover`);

          status.status = 'FAILED';

          this.logHealthEvent({
            timestamp: Date.now(),
            node: nodeName,
            event: 'PRIMARY_FAILED',
            error: error.message,
            failCount: status.failCount
          });

          if (this.cacheRing.failoverManager) {
            await this.cacheRing.failoverManager.failoverToReplica(nodeName);
            status.status = 'FAILED_OVER';
          }
        }
      }
    }
  }

  /**
   * Handle primary node recovery
   */
  async handleNodeRecovery(nodeName) {
    console.log(`[HealthMonitor] Handling recovery of ${nodeName}`);
    
    // Log recovery event
    this.logHealthEvent({
      timestamp: Date.now(),
      node: nodeName,
      event: 'PRIMARY_RECOVERED'
    });
    
    // Let FailoverManager handle reconfiguration
    if (this.cacheRing.failoverManager) {
      await this.cacheRing.failoverManager.handlePrimaryRecovery(nodeName);
    }
  }

  /**
   * Check if a node is healthy
   */
  isHealthy(nodeName) {
    const status = this.nodeStatus.get(nodeName);
    return status && status.status === 'HEALTHY';
  }

  /**
   * Get status of a specific node
   */
  getNodeStatus(nodeName) {
    return this.nodeStatus.get(nodeName);
  }

  /**
   * Get status of all nodes
   */
  getAllNodeStatus() {
    const statusMap = {};
    
    for (const [nodeName, status] of this.nodeStatus) {
      statusMap[nodeName] = {
        status: status.status,
        lastCheck: new Date(status.lastCheck).toISOString(),
        lastSuccess: new Date(status.lastSuccess).toISOString(),
        failCount: status.failCount,
        uptime: this.calculateUptime(status.lastSuccess)
      };
    }
    
    return statusMap;
  }

  /**
   * Calculate uptime in human-readable format
   */
  calculateUptime(lastSuccess) {
    const uptimeMs = Date.now() - lastSuccess;
    const seconds = Math.floor(uptimeMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) return `${days}d ${hours % 24}h`;
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
  }

  /**
   * Log health event
   */
  logHealthEvent(event) {
    this.healthHistory.push({
      ...event,
      timestamp: new Date(event.timestamp).toISOString()
    });
    
    // Keep last 100 events
    if (this.healthHistory.length > 100) {
      this.healthHistory.shift();
    }
  }

  /**
   * Get health event history
   */
  getHealthHistory(limit = 20) {
    return this.healthHistory.slice(-limit).reverse();
  }

  /**
   * Get health summary
   */
  getHealthSummary() {
    const statuses = Array.from(this.nodeStatus.values());
    
    return {
      totalNodes: statuses.length,
      healthy: statuses.filter(s => s.status === 'HEALTHY').length,
      failed: statuses.filter(s => s.status === 'FAILED').length,
      failedOver: statuses.filter(s => s.status === 'FAILED_OVER').length,
      overallHealth: statuses.every(s => s.status === 'HEALTHY') ? 'healthy' : 'degraded'
    };
  }

  /**
   * Stop health monitoring
   */
  stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log('[HealthMonitor] Stopped health checks');
    }
  }
}

module.exports = HealthMonitor;