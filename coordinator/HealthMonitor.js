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
    for (const [nodeName] of this.cacheRing.nodes) {
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