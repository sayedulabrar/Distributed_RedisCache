const crypto = require('crypto');
const redis = require('redis');

class ConsistentHashRingWithReplication {
  constructor(primaryNodes, replicaNodes, virtualNodeCount = 150, replicationMode = 'async') {
    this.hashSpace = Math.pow(2, 32);
    this.nodeCount = primaryNodes.length;
    this.virtualNodeCount = virtualNodeCount;
    this.replicationMode = replicationMode; // 'async' or 'sync'
    
    this.ring = new Map();              // position -> node name
    this.sortedKeys = [];               // sorted virtual node positions
    this.virtualNodeMap = new Map();    // node name -> virtual positions
    
    // Replication structures
    this.nodes = new Map();             // node name -> { id, primary, replica, config }
    
    console.log(`[ReplicationRing] Initializing with ${this.nodeCount} Redis nodes`);
    console.log(`[ReplicationRing] Replication mode: ${this.replicationMode}`);
    console.log(`[ReplicationRing] Virtual nodes per node: ${this.virtualNodeCount}`);
    console.log(`[ReplicationRing] Total virtual nodes: ${this.nodeCount * this.virtualNodeCount}`);
    
    // Initialize primary-replica pairs
    primaryNodes.forEach((primaryConfig, index) => {
      const replicaConfig = replicaNodes[index];
      this.addNodeWithReplica(index, primaryConfig, replicaConfig);
    });
  }

  /**
   * Hash function
   */
  hashFunction(key) {
    const hash = crypto.createHash('sha256');
    hash.update(key);
    const hashHex = hash.digest('hex');
    const hashInt = parseInt(hashHex.substring(0, 8), 16);
    return hashInt % this.hashSpace;
  }

  /**
   * Add a Redis node with primary and replica
   */
  addNodeWithReplica(id, primaryConfig, replicaConfig) {
    const nodeName = `cache_node_${id}`;
    
    // Create primary client
    const primaryClient = redis.createClient({
      socket: {
        host: primaryConfig.host,
        port: primaryConfig.port
      }
    });

    primaryClient.on('error', (err) => {
      console.error(`[ReplicationRing] Primary error on ${nodeName}:`, err);
    });

    primaryClient.on('connect', () => {
      console.log(`[ReplicationRing] Connected to ${nodeName} PRIMARY at ${primaryConfig.host}:${primaryConfig.port}`);
    });

    // Create replica client
    const replicaClient = redis.createClient({
      socket: {
        host: replicaConfig.host,
        port: replicaConfig.port
      }
    });

    replicaClient.on('error', (err) => {
      console.error(`[ReplicationRing] Replica error on ${nodeName}:`, err);
    });

    replicaClient.on('connect', () => {
      console.log(`[ReplicationRing] Connected to ${nodeName} REPLICA at ${replicaConfig.host}:${replicaConfig.port}`);
    });

    // Store node information with both primary and replica
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

    // Create virtual nodes
    // Virtual nodes map to the node name (which contains both primary and replica)
    const virtualPositions = [];
    
    for (let i = 0; i < this.virtualNodeCount; i++) {
      const virtualNodeName = `${nodeName}:vnode${i}`;
      let position = this.hashFunction(virtualNodeName);
      
      // Handle collisions
      while (this.ring.has(position)) {
        position = (position + 1) % this.hashSpace;
      }
      
      this.ring.set(position, nodeName);
      virtualPositions.push(position);
      this.sortedKeys.push(position);
    }
    
    this.sortedKeys.sort((a, b) => a - b);
    this.virtualNodeMap.set(nodeName, virtualPositions);
    
    console.log(`[ReplicationRing] Added ${nodeName} with ${virtualPositions.length} virtual nodes (primary + replica)`);
  }

  /**
   * Find the physical Redis node for a key (unchanged from Lab 3)
   */
  getNodeForKey(key) {
    if (this.sortedKeys.length === 0) {
      throw new Error('No nodes available in ring');
    }

    const keyPosition = this.hashFunction(key);
    
    // Binary search
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

  /**
   * Connect all Redis clients (primary and replica)
   */
  async connect() {
    console.log('[ReplicationRing] Connecting to all Redis nodes...');
    
    const connectPromises = [];
    
    this.nodes.forEach(node => {
      connectPromises.push(node.primary.client.connect());
      connectPromises.push(node.replica.client.connect());
    });
    
    await Promise.all(connectPromises);
    console.log('[ReplicationRing] All Redis nodes connected (primaries + replicas)');
  }

  /**
   * Disconnect all Redis clients
   */
  async disconnect() {
    console.log('[ReplicationRing] Disconnecting from all Redis nodes...');
    
    const disconnectPromises = [];
    
    this.nodes.forEach(node => {
      disconnectPromises.push(node.primary.client.quit());
      disconnectPromises.push(node.replica.client.quit());
    });
    
    await Promise.all(disconnectPromises);
    console.log('[ReplicationRing] All Redis nodes disconnected');
  }

  /**
   * SET operation with replication support
   */
  async set(key, value, ttl = null, mode = null) {
    const node = this.getNodeForKey(key);
    const valueStr = typeof value === 'string' ? value : JSON.stringify(value);
    const effectiveMode = mode || this.replicationMode;
    
    try {
      const startTime = Date.now();
      
      // Step 1: Write to primary
      if (ttl) {
        await node.primary.client.setEx(key, ttl, valueStr);
      } else {
        await node.primary.client.set(key, valueStr);
      }
      
      // Step 2: Handle replication based on mode
      let replicationInfo = {
        mode: effectiveMode,
        replicas: 0,
        status: 'async'
      };
      
      if (effectiveMode === 'sync') {
        // Synchronous replication: wait for replica confirmation
        try {
          const replicated = await node.primary.client.wait(1, 1000); // 1 replica, 1s timeout
          replicationInfo.replicas = replicated;
          replicationInfo.status = replicated >= 1 ? 'confirmed' : 'timeout';
          
          if (replicated < 1) {
            console.warn(`[ReplicationRing] Sync replication timeout for key: ${key}`);
          }
        } catch (error) {
          console.error(`[ReplicationRing] Sync replication error:`, error);
          replicationInfo.status = 'error';
          replicationInfo.error = error.message;
        }
      }
      
      const latency = Date.now() - startTime;
      
      return {
        success: true,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        hash: this.hashFunction(key),
        replication: replicationInfo,
        latency: `${latency}ms`
      };
      
    } catch (error) {
      console.error(`[ReplicationRing] SET error on ${node.name}:`, error);
      return {
        success: false,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        error: error.message
      };
    }
  }

  /**
   * GET operation (reads from primary only in Lab 4)
   */
  async get(key) {
    const node = this.getNodeForKey(key);
    
    try {
      const startTime = Date.now();
      
      // Read from primary (Lab 5 will add replica failover)
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
      
    } catch (error) {
      console.error(`[ReplicationRing] GET error on ${node.name}:`, error);
      return {
        success: false,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        error: error.message
      };
    }
  }

  /**
   * DELETE operation
   */
  async delete(key) {
    const node = this.getNodeForKey(key);
    
    try {
      // Delete from primary (replication handles replica)
      const result = await node.primary.client.del(key);
      
      return {
        success: result === 1,
        key: key,
        node: node.name,
        nodeIndex: node.id
      };
    } catch (error) {
      console.error(`[ReplicationRing] DELETE error on ${node.name}:`, error);
      return {
        success: false,
        key: key,
        node: node.name,
        nodeIndex: node.id,
        error: error.message
      };
    }
  }

  /**
   * Get comprehensive statistics with replication info
   */
  async getAllStats() {
    const nodeStats = await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          // Get primary stats
          const primaryInfo = await node.primary.client.info('replication');
          const primaryKeyspace = await node.primary.client.info('keyspace');
          const primaryStats = await node.primary.client.info('stats');
          
          // Get replica stats
          const replicaInfo = await node.replica.client.info('replication');
          const replicaKeyspace = await node.replica.client.info('keyspace');
          
          // Parse key counts
          const primaryKeysMatch = primaryKeyspace.match(/db0:keys=(\d+)/);
          const replicaKeysMatch = replicaKeyspace.match(/db0:keys=(\d+)/);
          
          const primaryKeys = primaryKeysMatch ? parseInt(primaryKeysMatch[1]) : 0;
          const replicaKeys = replicaKeysMatch ? parseInt(replicaKeysMatch[1]) : 0;
          
          // Parse hit/miss stats
          const hitsMatch = primaryStats.match(/keyspace_hits:(\d+)/);
          const missesMatch = primaryStats.match(/keyspace_misses:(\d+)/);
          
          const hits = hitsMatch ? parseInt(hitsMatch[1]) : 0;
          const misses = missesMatch ? parseInt(missesMatch[1]) : 0;
          const total = hits + misses;
          const hitRate = total > 0 ? ((hits / total) * 100).toFixed(2) : '0.00';
          
          // Check replication lag
          const replicaOffsetMatch = replicaInfo.match(/master_repl_offset:(\d+)/);
          const primaryOffsetMatch = primaryInfo.match(/master_repl_offset:(\d+)/);
          
          const replicaOffset = replicaOffsetMatch ? parseInt(replicaOffsetMatch[1]) : 0;
          const primaryOffset = primaryOffsetMatch ? parseInt(primaryOffsetMatch[1]) : 0;
          const replicationLag = Math.max(0, primaryOffset - replicaOffset);
          
          // Check replication connection status
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
          console.error(`[ReplicationRing] Stats error on ${node.name}:`, error);
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
    
    // Check overall replication health
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

  /**
   * Get replication lag details
   */
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

  /**
   * Get distribution (unchanged from Lab 3, but now shows replication)
   */
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

  /**
   * Get key mappings (unchanged from Lab 3)
   */
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
          console.error(`[ReplicationRing] Keys error on ${node.name}:`, error);
        }
      })
    );

    return mappings;
  }

  /**
   * Visualize ring
   */
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

  /**
   * Clear all nodes
   */
  async clearAll() {
    await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          await node.primary.client.flushDb();
          console.log(`[ReplicationRing] Cleared ${node.name} primary`);
        } catch (error) {
          console.error(`[ReplicationRing] Clear error on ${node.name}:`, error);
        }
      })
    );
  }
}

module.exports = ConsistentHashRingWithReplication;