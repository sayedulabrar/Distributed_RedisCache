const crypto = require('crypto');
const redis = require('redis');
const HealthMonitor = require('./HealthMonitor');
const FailoverManager = require('./FailoverManager');

class ConsistentHashRingWithReplication {
  constructor(primaryNodes, replicaNodes, virtualNodeCount = 150, replicationMode = 'async') {
    this.hashSpace = Math.pow(2, 32);
    this.nodeCount = primaryNodes.length;
    this.virtualNodeCount = virtualNodeCount;
    this.replicationMode = replicationMode;
    
    this.ring = new Map();
    this.sortedKeys = [];
    this.virtualNodeMap = new Map();
    this.nodes = new Map();
    
    console.log(`[HashRing] Initializing with ${this.nodeCount} Redis nodes`);
    console.log(`[HashRing] Replication mode: ${this.replicationMode}`);
    console.log(`[HashRing] Virtual nodes per node: ${this.virtualNodeCount}`);
    
    // Initialize primary-replica pairs
    primaryNodes.forEach((primaryConfig, index) => {
      const replicaConfig = replicaNodes[index];
      this.addNodeWithReplica(index, primaryConfig, replicaConfig);
    });

    // Initialize health monitor and failover manager
    this.healthMonitor = new HealthMonitor(this);
    this.failoverManager = new FailoverManager(this);
  }

  hashFunction(key) {
    const hash = crypto.createHash('sha256');
    hash.update(key);
    const hashHex = hash.digest('hex');
    const hashInt = parseInt(hashHex.substring(0, 8), 16);
    return hashInt % this.hashSpace;
  }

  addNodeWithReplica(id, primaryConfig, replicaConfig) {
    const nodeName = `cache_node_${id}`;
    
    const primaryClient = redis.createClient({
      socket: { host: primaryConfig.host, port: primaryConfig.port }
    });

    primaryClient.on('error', (err) => {
      console.error(`[HashRing] Primary error on ${nodeName}:`, err.message);
    });

    primaryClient.on('connect', () => {
      console.log(`[HashRing] Connected to ${nodeName} PRIMARY`);
    });

    const replicaClient = redis.createClient({
      socket: { host: replicaConfig.host, port: replicaConfig.port }
    });

    replicaClient.on('error', (err) => {
      console.error(`[HashRing] Replica error on ${nodeName}:`, err.message);
    });

    replicaClient.on('connect', () => {
      console.log(`[HashRing] Connected to ${nodeName} REPLICA`);
    });

    this.nodes.set(nodeName, {
      id: id,
      name: nodeName,
      primary: {
        client: primaryClient,
        host: primaryConfig.host,
        port: primaryConfig.port
      },
      replica: {
        client: replicaClient,
        host: replicaConfig.host,
        port: replicaConfig.port
      }
    });

    // Add virtual nodes to ring
    const virtualPositions = [];
    for (let i = 0; i < this.virtualNodeCount; i++) {
      const virtualNodeName = `${nodeName}:vnode${i}`;
      let position = this.hashFunction(virtualNodeName);
      
      while (this.ring.has(position)) {
        position = (position + 1) % this.hashSpace;
      }
      
      this.ring.set(position, nodeName);
      virtualPositions.push(position);
      this.sortedKeys.push(position);
    }
    
    this.sortedKeys.sort((a, b) => a - b);
    this.virtualNodeMap.set(nodeName, virtualPositions);
    
    console.log(`[HashRing] Added ${nodeName} with ${virtualPositions.length} virtual nodes`);
  }

  getNodeForKey(key) {
    if (this.sortedKeys.length === 0) {
      throw new Error('No nodes available in ring');
    }

    const keyPosition = this.hashFunction(key);
    let left = 0;
    let right = this.sortedKeys.length;

    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      if (this.sortedKeys[mid] < keyPosition) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    if (left >= this.sortedKeys.length) {
      left = 0;
    }

    const virtualNodePosition = this.sortedKeys[left];
    const physicalNodeName = this.ring.get(virtualNodePosition);
    
    return this.nodes.get(physicalNodeName);
  }

  async connect() {
    console.log('[HashRing] Connecting to all Redis nodes...');
    
    const connectPromises = [];
    this.nodes.forEach(node => {
      connectPromises.push(node.primary.client.connect());
      connectPromises.push(node.replica.client.connect());
    });
    
    await Promise.all(connectPromises);
    console.log('[HashRing] ✓ All Redis nodes connected');

    // Start health monitoring after connections established
    console.log('[HashRing] Starting health monitoring...');
    this.healthMonitor.startMonitoring();
  }

  async disconnect() {
    console.log('[HashRing] Disconnecting...');
    
    // Stop health monitoring first
    this.healthMonitor.stop();
    
    const disconnectPromises = [];
    this.nodes.forEach(node => {
      disconnectPromises.push(node.primary.client.quit().catch(() => {}));
      disconnectPromises.push(node.replica.client.quit().catch(() => {}));
    });
    
    await Promise.allSettled(disconnectPromises);
    console.log('[HashRing] ✓ Disconnected');
  }

  /**
   * GET with automatic failover
   */
  async get(key) {
    const node = this.getNodeForKey(key);
    const startTime = Date.now();
    
    // After failover, node.primary is the promoted replica (current active primary)
    // Try reading from current primary first
    try {
      const value = await node.primary.client.get(key);
      const latency = Date.now() - startTime;
      
      if (value === null) {
        return {
          success: false,
          key: key,
          node: node.name,
          nodeIndex: node.id,
          reason: 'key_not_found',
          source: 'primary',
          latency: `${latency}ms`
        };
      }

      let parsedValue;
      try {
        parsedValue = JSON.parse(value);
      } catch {
        parsedValue = value;
      }

      return {
        success: true,
        key: key,
        value: parsedValue,
        node: node.name,
        nodeIndex: node.id,
        source: 'primary',
        latency: `${latency}ms`
      };
    } catch (primaryError) {
      // Primary failed, try replica as fallback
      console.log(`[HashRing] Primary failed for ${key}, trying replica`);
      
      try {
        const value = await node.replica.client.get(key);
        const latency = Date.now() - startTime;
        
        if (value === null) {
          return {
            success: false,
            key: key,
            node: node.name,
            nodeIndex: node.id,
            reason: 'key_not_found',
            source: 'replica',
            failover: true,
            latency: `${latency}ms`
          };
        }

        let parsedValue;
        try {
          parsedValue = JSON.parse(value);
        } catch {
          parsedValue = value;
        }

        return {
          success: true,
          key: key,
          value: parsedValue,
          node: node.name,
          nodeIndex: node.id,
          source: 'replica',
          failover: true,
          warning: 'Primary unavailable, reading from replica',
          latency: `${latency}ms`
        };
      } catch (replicaError) {
        // Both primary and replica failed
        console.error(`[HashRing] Both primary and replica failed for ${key}`);
        const latency = Date.now() - startTime;
        
        return {
          success: false,
          key: key,
          node: node.name,
          nodeIndex: node.id,
          error: 'Node completely unavailable',
          latency: `${latency}ms`
        };
      }
    }
  }

  /**
   * SET with failover awareness
   */
  async set(key, value, ttl = null, mode = null) {
    const node = this.getNodeForKey(key);
    const valueStr = typeof value === 'string' ? value : JSON.stringify(value);
    const effectiveMode = mode || this.replicationMode;
    
    // Check if node is currently failing over
    if (this.failoverManager.isNodeInFailover(node.name)) {
      return {
        success: false,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        error: 'Node is in failover state',
        retryAfter: '5 seconds',
        reason: 'Promotion in progress, writes temporarily unavailable'
      };
    }
    
    try {
      const startTime = Date.now();
      
      // Get write target (could be original primary or promoted replica)
      const writeTarget = this.failoverManager.getWriteTarget(node.name);
      
      // Write to current primary (original or promoted)
      if (ttl) {
        await writeTarget.client.setEx(key, ttl, valueStr);
      } else {
        await writeTarget.client.set(key, valueStr);
      }
      
      // Handle replication if needed
      let replicationInfo = {
        mode: effectiveMode,
        replicas: 0,
        status: 'async'
      };
      
      if (effectiveMode === 'sync') {
        try {
          const replicated = await writeTarget.client.wait(1, 1000);
          replicationInfo.replicas = replicated;
          replicationInfo.status = replicated >= 1 ? 'confirmed' : 'timeout';
        } catch (error) {
          replicationInfo.status = 'error';
          replicationInfo.error = error.message;
        }
      }
      
      const latency = Date.now() - startTime;
      
      // Check if this is a promoted replica
      const failoverStatus = this.failoverManager.getNodeFailoverStatus(node.name);
      const isPromoted = failoverStatus.promoted;
      
      return {
        success: true,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        hash: this.hashFunction(key),
        target: isPromoted ? 'promoted_replica' : 'primary',
        replication: replicationInfo,
        latency: `${latency}ms`
      };
      
    } catch (error) {
      console.error(`[HashRing] SET error on ${node.name}:`, error.message);
      return {
        success: false,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        error: error.message
      };
    }
  }

  async delete(key) {
    const node = this.getNodeForKey(key);
    
    try {
      const writeTarget = this.failoverManager.getWriteTarget(node.name);
      const result = await writeTarget.client.del(key);
      
      return {
        success: result === 1,
        key: key,
        node: node.name,
        nodeIndex: node.id
      };
    } catch (error) {
      console.error(`[HashRing] DELETE error on ${node.name}:`, error.message);
      return {
        success: false,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        error: error.message
      };
    }
  }

  async getAllStats() {
    const nodeStats = await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          const primaryInfo = await node.primary.client.info('replication');
          const primaryKeyspace = await node.primary.client.info('keyspace');
          const primaryStats = await node.primary.client.info('stats');
          
          const replicaInfo = await node.replica.client.info('replication');
          const replicaKeyspace = await node.replica.client.info('keyspace');
          
          const primaryKeysMatch = primaryKeyspace.match(/db0:keys=(\d+)/);
          const replicaKeysMatch = replicaKeyspace.match(/db0:keys=(\d+)/);
          
          const primaryKeys = primaryKeysMatch ? parseInt(primaryKeysMatch[1]) : 0;
          const replicaKeys = replicaKeysMatch ? parseInt(replicaKeysMatch[1]) : 0;
          
          const hitsMatch = primaryStats.match(/keyspace_hits:(\d+)/);
          const missesMatch = primaryStats.match(/keyspace_misses:(\d+)/);
          
          const hits = hitsMatch ? parseInt(hitsMatch[1]) : 0;
          const misses = missesMatch ? parseInt(missesMatch[1]) : 0;
          const total = hits + misses;
          const hitRate = total > 0 ? ((hits / total) * 100).toFixed(2) : '0.00';
          
          const replicaOffsetMatch = replicaInfo.match(/master_repl_offset:(\d+)/);
          const primaryOffsetMatch = primaryInfo.match(/master_repl_offset:(\d+)/);
          
          const replicaOffset = replicaOffsetMatch ? parseInt(replicaOffsetMatch[1]) : 0;
          const primaryOffset = primaryOffsetMatch ? parseInt(primaryOffsetMatch[1]) : 0;
          const replicationLag = Math.max(0, primaryOffset - replicaOffset);
          
          const connectedReplicasMatch = primaryInfo.match(/connected_slaves:(\d+)/);
          const connectedReplicas = connectedReplicasMatch ? parseInt(connectedReplicasMatch[1]) : 0;
          
          return {
            nodeId: node.id,
            nodeName: node.name,
            primary: {
              host: node.primary.host,
              port: node.primary.port,
              keys: primaryKeys,
              role: 'master',
              hits: hits,
              misses: misses,
              hitRate: `${hitRate}%`
            },
            replica: {
              host: node.replica.host,
              port: node.replica.port,
              keys: replicaKeys,
              role: 'replica'
            },
            replication: {
              status: replicationLag === 0 ? 'synced' : 'lagging',
              lag: replicationLag,
              connectedReplicas: connectedReplicas,
              health: (connectedReplicas > 0 && replicaKeys === primaryKeys) ? 'healthy' : 'syncing'
            },
            virtualNodes: this.virtualNodeMap.get(node.name)?.length || 0
          };
          
        } catch (error) {
          console.error(`[HashRing] Stats error on ${node.name}:`, error.message);
          return {
            nodeId: node.id,
            nodeName: node.name,
            error: error.message
          };
        }
      })
    );

    const totalPrimaryKeys = nodeStats.reduce((sum, s) => sum + (s.primary?.keys || 0), 0);
    const totalReplicaKeys = nodeStats.reduce((sum, s) => sum + (s.replica?.keys || 0), 0);
    const totalHits = nodeStats.reduce((sum, s) => sum + (s.primary?.hits || 0), 0);
    const totalMisses = nodeStats.reduce((sum, s) => sum + (s.primary?.misses || 0), 0);
    const overallHitRate = totalHits + totalMisses > 0
      ? ((totalHits / (totalHits + totalMisses)) * 100).toFixed(2)
      : '0.00';
    
    const allSynced = nodeStats.every(n => n.replication?.status === 'synced');

    return {
      nodeCount: this.nodes.size,
      replicationMode: this.replicationMode,
      totalPrimaryKeys: totalPrimaryKeys,
      totalReplicaKeys: totalReplicaKeys,
      overallHitRate: `${overallHitRate}%`,
      virtualNodesPerNode: this.virtualNodeCount,
      totalVirtualNodes: this.sortedKeys.length,
      replicationHealth: allSynced ? 'all_synced' : 'some_lagging',
      nodes: nodeStats
    };
  }

  async getReplicationLag() {
    const stats = await this.getAllStats();
    
    const lagInfo = stats.nodes.map(n => ({
      node: n.nodeName,
      lag: n.replication?.lag || 0,
      status: n.replication?.status || 'unknown',
      health: n.replication?.health || 'unknown'
    }));
    
    const maxLag = Math.max(...lagInfo.map(l => l.lag));
    const avgLag = lagInfo.reduce((sum, l) => sum + l.lag, 0) / lagInfo.length;
    
    return {
      nodes: lagInfo,
      maxLag: maxLag,
      avgLag: Math.round(avgLag),
      allSynced: maxLag === 0
    };
  }

  async getDistribution() {
    const stats = await this.getAllStats();
    
    const distribution = stats.nodes.map(node => ({
      nodeId: node.nodeId,
      nodeName: node.nodeName,
      primaryKeys: node.primary?.keys || 0,
      replicaKeys: node.replica?.keys || 0,
      virtualNodes: node.virtualNodes || 0,
      replicationStatus: node.replication?.status || 'unknown',
      percentage: 0
    }));

    const totalKeys = distribution.reduce((sum, d) => sum + d.primaryKeys, 0);
    
    if (totalKeys > 0) {
      distribution.forEach(d => {
        d.percentage = ((d.primaryKeys / totalKeys) * 100).toFixed(2);
      });
    }

    return distribution;
  }

  async getKeyMappings() {
    const mappings = new Map();
    
    await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          const keys = await node.primary.client.keys('*');
          keys.forEach(key => {
            mappings.set(key, {
              nodeIndex: node.id,
              nodeName: node.name
            });
          });
        } catch (error) {
          console.error(`[HashRing] Keys error on ${node.name}:`, error.message);
        }
      })
    );

    return mappings;
  }

  visualizeRing() {
    const nodeRanges = new Map();
    
    this.sortedKeys.forEach((position, index) => {
      const physicalNodeName = this.ring.get(position);
      const nextPosition = this.sortedKeys[(index + 1) % this.sortedKeys.length];
      
      let range;
      if (nextPosition > position) {
        range = nextPosition - position;
      } else {
        range = (this.hashSpace - position) + nextPosition;
      }
      
      if (!nodeRanges.has(physicalNodeName)) {
        nodeRanges.set(physicalNodeName, 0);
      }
      nodeRanges.set(physicalNodeName, nodeRanges.get(physicalNodeName) + range);
    });

    const visualization = [];
    nodeRanges.forEach((range, nodeName) => {
      const percentage = (range / this.hashSpace * 100).toFixed(2);
      const virtualNodeCount = this.virtualNodeMap.get(nodeName)?.length || 0;
      
      visualization.push({
        nodeName: nodeName,
        virtualNodes: virtualNodeCount,
        rangeSize: range,
        percentage: percentage + '%'
      });
    });

    return {
      totalVirtualNodes: this.sortedKeys.length,
      virtualNodesPerPhysicalNode: this.virtualNodeCount,
      physicalNodes: visualization
    };
  }

  async clearAll() {
    await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          await node.primary.client.flushDb();
          console.log(`[HashRing] Cleared ${node.name} primary`);
        } catch (error) {
          console.error(`[HashRing] Clear error on ${node.name}:`, error.message);
        }
      })
    );
  }
}

module.exports = ConsistentHashRingWithReplication;