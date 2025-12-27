class FailoverManager {
  constructor(cacheRing) {
    this.cacheRing = cacheRing;
    this.failoverStatus = new Map(); // nodeName -> { status, timestamp, promoted, duration }
    this.failoverMetrics = {
      totalFailovers: 0,
      successfulFailovers: 0,
      failedFailovers: 0,
      totalFailoverTime: 0,
      averageFailoverTime: 0
    };
  }

  /**
   * Execute failover to replica
   */
  async failoverToReplica(nodeName) {
    console.log(`[FailoverManager] Starting failover for ${nodeName}`);
    
    const startTime = Date.now();
    
    try {
      // Step 1: Mark node as in-failover
      this.failoverStatus.set(nodeName, {
        status: 'FAILING_OVER',
        timestamp: Date.now(),
        promoted: false
      });

      // Step 2: Verify replica is accessible
      const node = this.cacheRing.nodes.get(nodeName);
      await node.replica.client.ping();
      console.log(`[FailoverManager] ✓ Replica for ${nodeName} is accessible`);

      // Step 3: Promote replica to primary
      await this.promoteReplica(nodeName);

      // Step 4: Update failover status
      const duration = Date.now() - startTime;
      
      this.failoverStatus.set(nodeName, {
        status: 'FAILED_OVER',
        timestamp: Date.now(),
        promoted: true,
        duration: duration
      });

      // Update metrics
      this.failoverMetrics.totalFailovers++;
      this.failoverMetrics.successfulFailovers++;
      this.failoverMetrics.totalFailoverTime += duration;
      this.updateAverageFailoverTime();

      console.log(
        `[FailoverManager] ✓ Failover complete for ${nodeName} ` + `(${duration}ms) - Replica promoted to primary`
      );
      
      return { success: true, duration, nodeName };
      
    } catch (error) {
      console.error(`[FailoverManager] ✗ Failover failed for ${nodeName}:`, error.message);
      
      this.failoverStatus.set(nodeName, {
        status: 'FAILOVER_FAILED',
        timestamp: Date.now(),
        error: error.message
      });

      this.failoverMetrics.failedFailovers++;
      
      return { success: false, error: error.message, nodeName };
    }
  }

  /**
   * Promote replica to primary
   */
  async promoteReplica(nodeName) {
    const node = this.cacheRing.nodes.get(nodeName);
    
    console.log(`[FailoverManager] Promoting replica to primary for ${nodeName}`);
    
    try {
      // Make replica writable
      await node.replica.client.configSet('replica-read-only', 'no');
      console.log(`[FailoverManager]   → Replica set to writable`);
      
      // Make replica standalone (break replication link)
      await node.replica.client.replicaOf('NO', 'ONE');
      console.log(`[FailoverManager]   → Replica promoted to standalone`);
      
      // Swap primary and replica in coordinator's memory
      const tempPrimary = node.primary;
      node.primary = node.replica;
      node.replica = tempPrimary; // Old primary (now offline) becomes "replica"
      
      console.log(`[FailoverManager] ✓ Replica promoted successfully for ${nodeName}`);
      
    } catch (error) {
      console.error(`[FailoverManager] ✗ Promotion failed for ${nodeName}:`, error.message);
      throw error;
    }
  }

  /**
   * Handle primary recovery
   */
  async handlePrimaryRecovery(nodeName) {
    console.log(`[FailoverManager] Handling primary recovery for ${nodeName}`);
    
    const node = this.cacheRing.nodes.get(nodeName);
    const failoverInfo = this.failoverStatus.get(nodeName);
    
    if (!failoverInfo || !failoverInfo.promoted) {
      console.log(`[FailoverManager] No failover to recover from for ${nodeName}`);
      return;
    }
    
    try {
      // After promotion, roles are swapped:
      // - node.primary = promoted replica (now acting as primary)
      // - node.replica = recovered old primary (needs reconfiguration)
      
      const recoveredNode = node.replica;
      const currentPrimary = node.primary;
      
      console.log(
        `[FailoverManager] Reconfiguring recovered node as replica of promoted node...`
      );
      
      // Make recovered node a replica of the promoted node
      await recoveredNode.client.replicaOf(
        currentPrimary.host,
        currentPrimary.port
      );
      
      // Ensure it's read-only
      await recoveredNode.client.configSet('replica-read-only', 'yes');
      
      console.log(
        `[FailoverManager] ✓ Recovery complete for ${nodeName} - ` +
        `New topology: promoted replica is primary, old primary is replica`
      );
      
      // Update status - keep the new topology
      this.failoverStatus.set(nodeName, {
        status: 'RECOVERED',
        timestamp: Date.now(),
        topology: 'promoted_replica_is_primary',
        note: 'Old primary reconfigured as replica'
      });
      
    } catch (error) {
      console.error(
        `[FailoverManager] ✗ Recovery reconfiguration failed for ${nodeName}:`,
        error.message
      );
    }
  }

  /**
   * Check if node is currently failing over
   */
  isNodeInFailover(nodeName) {
    const status = this.failoverStatus.get(nodeName);
    return status && status.status === 'FAILING_OVER';
  }

  /**
   * Get the correct write target for a node
   */
  getWriteTarget(nodeName) {
    const node = this.cacheRing.nodes.get(nodeName);
    
    // After failover, node.primary is the promoted replica
    // This method ensures writes always go to the current primary
    return node.primary;
  }

  /**
   * Get failover status for a specific node
   */
  getNodeFailoverStatus(nodeName) {
    return this.failoverStatus.get(nodeName) || {
      status: 'NEVER_FAILED',
      promoted: false
    };
  }

  /**
   * Update average failover time
   */
  updateAverageFailoverTime() {
    if (this.failoverMetrics.successfulFailovers > 0) {
      this.failoverMetrics.averageFailoverTime = 
        this.failoverMetrics.totalFailoverTime / 
        this.failoverMetrics.successfulFailovers;
    }
  }

  /**
   * Get comprehensive failover metrics
   */
  getFailoverMetrics() {
    return {
      ...this.failoverMetrics,
      averageFailoverTime: `${Math.round(this.failoverMetrics.averageFailoverTime)}ms`,
      successRate: this.failoverMetrics.totalFailovers > 0
        ? `${((this.failoverMetrics.successfulFailovers / this.failoverMetrics.totalFailovers) * 100).toFixed(1)}%`
        : 'N/A',
      nodeStatus: Array.from(this.failoverStatus.entries()).map(([name, status]) => ({
        node: name,
        ...status,
        timestamp: new Date(status.timestamp).toISOString()
      }))
    };
  }

  /**
   * Reset failover status for a node (testing purposes)
   */
  resetNodeStatus(nodeName) {
    this.failoverStatus.delete(nodeName);
    console.log(`[FailoverManager] Reset failover status for ${nodeName}`);
  }
}

module.exports = FailoverManager;