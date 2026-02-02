const crypto = require('crypto');
const redis = require('redis');

class ConsistentHashRingWithVNodes {
  constructor(redisNodes, virtualNodeCount = 150) {
    this.hashSpace = Math.pow(2, 32); // 4,294,967,296 positions
    this.nodeCount = redisNodes.length;
    this.virtualNodeCount = virtualNodeCount;
    
    // Store mappings
    this.ring = new Map();              // position -> physical node name
    this.nodes = new Map();             // node name -> { id, client, host, port }
    this.sortedKeys = [];               // sorted array of all virtual node positions
    this.virtualNodeMap = new Map();    // node name -> [virtual positions]
    
    console.log(`[VNodeHashRing] Initializing with ${this.nodeCount} Redis nodes`);
    console.log(`[VNodeHashRing] Virtual nodes per node: ${this.virtualNodeCount}`);
    console.log(`[VNodeHashRing] Total virtual nodes: ${this.nodeCount * this.virtualNodeCount}`);
    console.log(`[VNodeHashRing] Hash space: 0 to ${this.hashSpace - 1}`);
    
    // Create Redis clients and add virtual nodes to ring
    redisNodes.forEach((nodeConfig, index) => {
      this.addNode(index, nodeConfig);
    });
  }

  /**
   * Hash function using SHA-256
   * Returns a position in the range [0, 2^32 - 1]
   */
  hashFunction(key) {
    const hash = crypto.createHash('sha256');
    hash.update(key);
    const hashHex = hash.digest('hex');
    
    // Take first 8 characters (32 bits) and convert to integer
    const hashInt = parseInt(hashHex.substring(0, 8), 16);
    return hashInt % this.hashSpace;
  }

  /**
   * Add a Redis node with virtual nodes to the ring
   */
  addNode(id, nodeConfig) {
    const nodeName = `cache_node_${id}`;
    
    // Create Redis client
    const client = redis.createClient({
      socket: {
        host: nodeConfig.host,
        port: nodeConfig.port
      }
    });

    client.on('error', (err) => {
      console.error(`[VNodeHashRing] Redis error on ${nodeName}:`, err);
    });

    client.on('connect', () => {
      console.log(`[VNodeHashRing] Connected to ${nodeName} at ${nodeConfig.host}:${nodeConfig.port}`);
    });

    // Store node information
    this.nodes.set(nodeName, {
      id: id,
      name: nodeName,
      host: nodeConfig.host,
      port: nodeConfig.port,
      client: client
    });

    // Create virtual nodes
    const virtualPositions = [];
    
    for (let i = 0; i < this.virtualNodeCount; i++) {
      const virtualNodeName = `${nodeName}:vnode${i}`;
      let position = this.hashFunction(virtualNodeName);
      
      // Handle collisions (rare but possible)
      // Simply increment position until we find an empty spot
      while (this.ring.has(position)) {
        position = (position + 1) % this.hashSpace;
      }
      
      // Add virtual node to ring (maps to physical node)
      this.ring.set(position, nodeName);
      virtualPositions.push(position);
      
      // Add to sorted keys
      this.sortedKeys.push(position);
    }
    
    // Keep sorted keys sorted
    this.sortedKeys.sort((a, b) => a - b);
    
    // Store virtual positions for this physical node
    this.virtualNodeMap.set(nodeName, virtualPositions);
    
    console.log(`[VNodeHashRing] Added ${nodeName} with ${virtualPositions.length} virtual nodes`);
  }

  /**
   * Remove a Redis node (and all its virtual nodes) from the ring
   * Handles data migration from the removed node to the next responsible node
   */
  async removeNode(nodeName) {
    const nodeInfo = this.nodes.get(nodeName);
    if (!nodeInfo) {
      throw new Error(`Node ${nodeName} not found`);
    }

    // Get all virtual positions for this node
    const virtualPositions = this.virtualNodeMap.get(nodeName);
    if (!virtualPositions) {
      throw new Error(`Virtual positions for ${nodeName} not found`);
    }

    // STEP 1: Migrate data from the removed node to their new responsible nodes
    console.log(`[VNodeHashRing] Starting data migration from ${nodeName}...`);
    await this.migrateDataFromRemovedNode(nodeInfo, virtualPositions);

    // STEP 2: Remove all virtual nodes from the ring
    virtualPositions.forEach(position => {
      this.ring.delete(position);
    });

    // STEP 3: Remove from sorted keys
    this.sortedKeys = this.sortedKeys.filter(
      pos => !virtualPositions.includes(pos)
    );

    // STEP 4: Disconnect Redis client
    await nodeInfo.client.quit();

    // STEP 5: Clean up
    this.virtualNodeMap.delete(nodeName);
    this.nodes.delete(nodeName);
    
    console.log(`[VNodeHashRing] Removed ${nodeName} and its ${virtualPositions.length} virtual nodes`);
  }

  /**
   * Migrate all data from a removed node to the next node clockwise in the ring
   */
  async migrateDataFromRemovedNode(nodeInfo, virtualPositions) {
    try {
      // Get all keys from the node being removed
      const keys = await nodeInfo.client.keys('*');
      
      if (keys.length === 0) {
        console.log(`[VNodeHashRing] No keys to migrate from ${nodeInfo.name}`);
        return;
      }

      console.log(`[VNodeHashRing] Migrating ${keys.length} keys from ${nodeInfo.name}`);

      let migratedCount = 0;
      let failedCount = 0;

      // For each virtual position of the removed node, find the next position clockwise
      // All keys owned by this position go to the next position
      const positionTransferMap = new Map();
      
      virtualPositions.forEach(position => {
        // Find the index of this position in sortedKeys
        const index = this.sortedKeys.indexOf(position);
        if (index >= 0) {
          // Get the next position clockwise
          const nextIndex = (index + 1) % this.sortedKeys.length;
          const nextPosition = this.sortedKeys[nextIndex];
          const nextNodeName = this.ring.get(nextPosition);
          
          positionTransferMap.set(position, nextNodeName);
        }
      });

      // Migrate all keys
      for (const key of keys) {
        try {
          // Get the current value and TTL from the removed node
          const value = await nodeInfo.client.get(key);
          const ttl = await nodeInfo.client.ttl(key);
          
          // Find which virtual position of the removed node this key belongs to
          const keyPosition = this.hashFunction(key);
          
          // Find the virtual position that owns this key (the one <= keyPosition, closest)
          let owningPosition = null;
          for (let i = virtualPositions.length - 1; i >= 0; i--) {
            if (virtualPositions[i] <= keyPosition) {
              owningPosition = virtualPositions[i];
              break;
            }
          }
          
          // If no position found, use the largest position (wraps around)
          if (owningPosition === null && virtualPositions.length > 0) {
            owningPosition = virtualPositions[virtualPositions.length - 1];
          }

          // Get the next node for this position
          const nextNodeName = positionTransferMap.get(owningPosition);
          const nextNode = this.nodes.get(nextNodeName);

          if (nextNode) {
            // Migrate the key to the next node
            if (ttl > 0) {
              // Key has a TTL, preserve it
              await nextNode.client.setEx(key, ttl, value);
            } else {
              // Key has no TTL (persistent)
              await nextNode.client.set(key, value);
            }

            migratedCount++;
            
            // Delete from removed node
            await nodeInfo.client.del(key);
          }
        } catch (error) {
          console.error(`[VNodeHashRing] Failed to migrate key ${key}:`, error.message);
          failedCount++;
        }
      }

      console.log(`[VNodeHashRing] Migration complete: ${migratedCount} keys migrated, ${failedCount} failed`);
    } catch (error) {
      console.error(`[VNodeHashRing] Error during data migration:`, error);
      throw new Error(`Failed to migrate data from ${nodeInfo.name}: ${error.message}`);
    }
  }

  /**
   * Find the physical Redis node for a key
   * 
   * Process:
   * 1. Hash the key to get its position
   * 2. Binary search to find the first virtual node >= key position
   * 3. Look up which physical node owns that virtual node
   * 4. Return the physical Redis node
   */
  getNodeForKey(key) {
    if (this.sortedKeys.length === 0) {
      throw new Error('No nodes available in ring');
    }

    // Hash the key to find its position
    const keyPosition = this.hashFunction(key);
    
    // Binary search for the first virtual node >= keyPosition
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

    // Wrap around if we reached the end
    if (left >= this.sortedKeys.length) {
      left = 0;
    }

    // Get the virtual node position
    const virtualNodePosition = this.sortedKeys[left];
    
    // Map virtual node to physical node
    const physicalNodeName = this.ring.get(virtualNodePosition);
    
    // Return the physical Redis node
    return this.nodes.get(physicalNodeName);
  }

  /**
   * Connect all Redis clients
   */
  async connect() {
    console.log('[VNodeHashRing] Connecting to all Redis nodes...');
    const connectPromises = Array.from(this.nodes.values()).map(node => node.client.connect());
    await Promise.all(connectPromises);
    console.log('[VNodeHashRing] All Redis nodes connected');
  }

  /**
   * Disconnect all Redis clients
   */
  async disconnect() {
    console.log('[VNodeHashRing] Disconnecting from all Redis nodes...');
    const disconnectPromises = Array.from(this.nodes.values()).map(node => node.client.quit());
    await Promise.all(disconnectPromises);
    console.log('[VNodeHashRing] All Redis nodes disconnected');
  }

  /**
   * SET operation: Store a key-value pair
   */
  async set(key, value, ttl = null) {
    const node = this.getNodeForKey(key);
    const valueStr = typeof value === 'string' ? value : JSON.stringify(value);
    
    try {
      if (ttl) {
        await node.client.setEx(key, ttl, valueStr);
      } else {
        await node.client.set(key, valueStr);
      }

      return {
        success: true,
        node: node.name,
        nodeIndex: node.id,
        key: key,
        hash: this.hashFunction(key)
      };
    } catch (error) {
      console.error(`[VNodeHashRing] SET error on ${node.name}:`, error);
      return {
        success: false,
        node: node.name,
        nodeIndex: node.id,
        key: key,
        error: error.message
      };
    }
  }

  /**
   * GET operation: Retrieve a value
   */
  async get(key) {
    const node = this.getNodeForKey(key);
    
    try {
      const value = await node.client.get(key);

      if (value === null) {
        return {
          success: false,
          node: node.name,
          nodeIndex: node.id,
          key: key,
          reason: 'key_not_found'
        };
      }

      // Try to parse JSON, fallback to string
      let parsedValue;
      try {
        parsedValue = JSON.parse(value);
      } catch {
        parsedValue = value;
      }

      return {
        success: true,
        node: node.name,
        nodeIndex: node.id,
        key: key,
        value: parsedValue
      };
    } catch (error) {
      console.error(`[VNodeHashRing] GET error on ${node.name}:`, error);
      return {
        success: false,
        node: node.name,
        nodeIndex: node.id,
        key: key,
        error: error.message
      };
    }
  }

  /**
   * DELETE operation: Remove a key
   */
  async delete(key) {
    const node = this.getNodeForKey(key);
    
    try {
      const result = await node.client.del(key);
      
      return {
        success: result === 1,
        node: node.name,
        nodeIndex: node.id,
        key: key
      };
    } catch (error) {
      console.error(`[VNodeHashRing] DELETE error on ${node.name}:`, error);
      return {
        success: false,
        node: node.name,
        nodeIndex: node.id,
        key: key,
        error: error.message
      };
    }
  }

  /**
   * Get statistics for all Redis nodes
   */
  async getAllStats() {
    const nodeStats = await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          const info = await node.client.info('stats');
          const keyspace = await node.client.info('keyspace');
          
          // Parse keyspace info to get key count
          const dbMatch = keyspace.match(/db0:keys=(\d+)/);
          const keyCount = dbMatch ? parseInt(dbMatch[1]) : 0;
          
          // Parse stats info
          const hitsMatch = info.match(/keyspace_hits:(\d+)/);
          const missesMatch = info.match(/keyspace_misses:(\d+)/);
          
          const hits = hitsMatch ? parseInt(hitsMatch[1]) : 0;
          const misses = missesMatch ? parseInt(missesMatch[1]) : 0;
          const total = hits + misses;
          const hitRate = total > 0 ? ((hits / total) * 100).toFixed(2) : '0.00';

          // Count virtual nodes for this physical node
          const virtualNodeCount = this.virtualNodeMap.get(node.name)?.length || 0;

          return {
            nodeId: node.id,
            nodeName: node.name,
            host: node.host,
            port: node.port,
            keys: keyCount,
            hits: hits,
            misses: misses,
            hitRate: `${hitRate}%`,
            virtualNodes: virtualNodeCount
          };
        } catch (error) {
          console.error(`[VNodeHashRing] Stats error on ${node.name}:`, error);
          return {
            nodeId: node.id,
            nodeName: node.name,
            host: node.host,
            port: node.port,
            error: error.message
          };
        }
      })
    );

    const totalKeys = nodeStats.reduce((sum, s) => sum + (s.keys || 0), 0);
    const totalHits = nodeStats.reduce((sum, s) => sum + (s.hits || 0), 0);
    const totalMisses = nodeStats.reduce((sum, s) => sum + (s.misses || 0), 0);
    const overallHitRate = totalHits + totalMisses > 0
      ? ((totalHits / (totalHits + totalMisses)) * 100).toFixed(2)
      : '0.00';

    return {
      nodeCount: this.nodes.size,
      totalKeys: totalKeys,
      overallHitRate: `${overallHitRate}%`,
      virtualNodesPerNode: this.virtualNodeCount,
      totalVirtualNodes: this.sortedKeys.length,
      nodes: nodeStats
    };
  }

  /**
   * Get distribution of keys across Redis nodes
   */
  async getDistribution() {
    const stats = await this.getAllStats();
    
    const distribution = stats.nodes.map(node => ({
      nodeId: node.nodeId,
      nodeName: node.nodeName,
      keyCount: node.keys || 0,
      virtualNodes: node.virtualNodes || 0,
      percentage: 0
    }));

    const totalKeys = distribution.reduce((sum, d) => sum + d.keyCount, 0);
    
    if (totalKeys > 0) {
      distribution.forEach(d => {
        d.percentage = ((d.keyCount / totalKeys) * 100).toFixed(2);
      });
    }

    return distribution;
  }

  /**
   * Get mapping of all keys to their Redis nodes
   */
  async getKeyMappings() {
    const mappings = new Map();
    
    await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          const keys = await node.client.keys('*');
          keys.forEach(key => {
            mappings.set(key, {
              nodeIndex: node.id,
              nodeName: node.name
            });
          });
        } catch (error) {
          console.error(`[VNodeHashRing] Keys error on ${node.name}:`, error);
        }
      })
    );

    return mappings;
  }

  /**
   * Visualize the ring with virtual nodes
   */
  visualizeRing() {
    // Group virtual nodes by physical node
    const nodeRanges = new Map();
    
    this.sortedKeys.forEach((position, index) => {
      const physicalNodeName = this.ring.get(position);
      const nextPosition = this.sortedKeys[(index + 1) % this.sortedKeys.length];
      
      // Calculate range for this virtual node
      let range;
      if (nextPosition > position) {
        range = nextPosition - position;
      } else {
        range = (this.hashSpace - position) + nextPosition;
      }
      
      // Add to physical node's total range
      if (!nodeRanges.has(physicalNodeName)) {
        nodeRanges.set(physicalNodeName, 0);
      }
      nodeRanges.set(physicalNodeName, nodeRanges.get(physicalNodeName) + range);
    });

    // Calculate percentages
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
   * Clear all Redis nodes
   */
  async clearAll() {
    await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          await node.client.flushDb();
          console.log(`[VNodeHashRing] Cleared ${node.name}`);
        } catch (error) {
          console.error(`[VNodeHashRing] Clear error on ${node.name}:`, error);
        }
      })
    );
  }
}

module.exports = ConsistentHashRingWithVNodes;
