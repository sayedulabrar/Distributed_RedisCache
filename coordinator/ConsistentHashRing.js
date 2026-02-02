const crypto = require('crypto');
const redis = require('redis');

class ConsistentHashRing {
  constructor(redisNodes) {
    this.hashSpace = Math.pow(2, 32); // 4,294,967,296 positions
    this.nodeCount = redisNodes.length;
    
    // Store node positions and Redis clients
    this.ring = new Map();        // position -> node name
    this.nodes = new Map();       // node name -> { id, client, host, port }
    this.sortedKeys = [];         // sorted array of positions
    
    console.log(`[ConsistentHashRing] Initializing with ${this.nodeCount} Redis nodes`);
    console.log(`[ConsistentHashRing] Hash space: 0 to ${this.hashSpace - 1}`);
    
    // Create Redis clients and add to ring
    redisNodes.forEach((nodeConfig, index) => {
      this.addNode(index, nodeConfig);
    });
  }

  /**
   * Hash function using SHA-256
   * Returns a position in the range [0, 2^32 - 1]
   * SHA-256 .digest('hex') returns hexadecimal, t convert to decimal we need to tell parseint what format it is, that's 16.
   */
  hashFunction(key) {
    return parseInt(crypto.createHash('sha256').update(key).digest('hex').slice(0, 8), 16) % this.hashSpace;
  }


  /**
   * Add a Redis node to the ring
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
      console.error(`[ConsistentHashRing] Redis error on ${nodeName}:`, err);
    });

    client.on('connect', () => {
      console.log(`[ConsistentHashRing] Connected to ${nodeName} at ${nodeConfig.host}:${nodeConfig.port}`);
    });

    // Store node information
    this.nodes.set(nodeName, {
      id: id,
      name: nodeName,
      host: nodeConfig.host,
      port: nodeConfig.port,
      client: client
    });

    // Calculate position on the ring
    const position = this.hashFunction(nodeName);
    
    // Check for collision (unlikely but possible)
    if (this.ring.has(position)) {
      console.warn(`[ConsistentHashRing] Collision at position ${position} for ${nodeName}`);
      // In production, you'd handle this by rehashing with a salt
      return;
    }
    
    // Add to ring
    this.ring.set(position, nodeName);
    
    // Add to sorted keys and keep sorted
    this.sortedKeys.push(position);
    this.sortedKeys.sort((a, b) => a - b);
    
    console.log(`[ConsistentHashRing] Added ${nodeName} at position ${position}`);
  }

  /**
   * Remove a Redis node from the ring
   */
  async removeNode(nodeName) {
    const nodeInfo = this.nodes.get(nodeName);
    if (!nodeInfo) {
      throw new Error(`Node ${nodeName} not found`);
    }

    // Find the position of this node
    let position = null;
    for (let [pos, name] of this.ring.entries()) {
      if (name === nodeName) {
        position = pos;
        break;
      }
    }

    if (position === null) {
      throw new Error(`Position for ${nodeName} not found on ring`);
    }

    // Disconnect Redis client
    await nodeInfo.client.quit();

    // Remove from ring
    this.ring.delete(position);
    
    // Remove from sorted keys
    this.sortedKeys = this.sortedKeys.filter(k => k !== position);
    
    // Remove from nodes
    this.nodes.delete(nodeName);
    
    console.log(`[ConsistentHashRing] Removed ${nodeName} from position ${position}`);
  }

  /**
   * Find the Redis node responsible for a key using binary search
    // For first>= we will use 
    //     if A[mid] >= target:
    //         ans = mid
    //         right = mid - 1   // search earlier
    // but for Last element ≤ target
    //     if A[mid] <= target:
    //         ans = mid
    //         left = mid + 1   // search later
   */
  getNodeForKey(key) {
    if (this.sortedKeys.length === 0) {
      throw new Error('No nodes available in ring');
    }

    const keyPosition = this.hashFunction(key);

    let left = 0;
    let right = this.sortedKeys.length - 1;
    let ans = -1;

    // lower-bound search: first >= target
    while (left <= right) {
      const mid = left + Math.floor((right - left) / 2);

      if (this.sortedKeys[mid] >= keyPosition) {
        ans = mid;          // candidate
        right = mid - 1;   // keep searching left side
      } else {
        left = mid + 1;
      }
    }

    // wrap-around if not found
    if (ans === -1) ans = 0;

    const nodePosition = this.sortedKeys[ans];
    const nodeName = this.ring.get(nodePosition);
    return this.nodes.get(nodeName);
  }

  /**
   * Connect all Redis clients
   */
  async connect() {
    console.log('[ConsistentHashRing] Connecting to all Redis nodes...');
    const connectPromises = Array.from(this.nodes.values()).map(node => node.client.connect());
    await Promise.all(connectPromises);
    console.log('[ConsistentHashRing] All Redis nodes connected');
  }

  /**
   * Disconnect all Redis clients
   */
  async disconnect() {
    console.log('[ConsistentHashRing] Disconnecting from all Redis nodes...');
    const disconnectPromises = Array.from(this.nodes.values()).map(node => node.client.quit());
    await Promise.all(disconnectPromises);
    console.log('[ConsistentHashRing] All Redis nodes disconnected');
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
      console.error(`[ConsistentHashRing] SET error on ${node.name}:`, error);
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
      console.error(`[ConsistentHashRing] GET error on ${node.name}:`, error);
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
      console.error(`[ConsistentHashRing] DELETE error on ${node.name}:`, error);
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

          return {
            nodeId: node.id,
            nodeName: node.name,
            host: node.host,
            port: node.port,
            keys: keyCount,
            hits: hits,
            misses: misses,
            hitRate: `${hitRate}%`
          };
        } catch (error) {
          console.error(`[ConsistentHashRing] Stats error on ${node.name}:`, error);
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
          console.error(`[ConsistentHashRing] Keys error on ${node.name}:`, error);
        }
      })
    );

    return mappings;
  }

  /**
   * Visualize the ring (for debugging)
   */
  visualizeRing() {
    const visualization = [];
    
    this.sortedKeys.forEach((position, index) => {
      const nodeName = this.ring.get(position);
      const nextPosition = this.sortedKeys[(index + 1) % this.sortedKeys.length];
      
      // Calculate the range this node covers
      let range;
      if (nextPosition > position) {
        range = nextPosition - position;
      } else {
        // Wrap around case
        range = (this.hashSpace - position) + nextPosition;
      }
      
      const percentage = (range / this.hashSpace * 100).toFixed(2);
      
      visualization.push({
        position: position,
        nodeName: nodeName,
        rangeSize: range,
        percentage: percentage + '%'
      });
    });

    return visualization;
  }

  /**
   * Clear all Redis nodes
   */
  async clearAll() {
    await Promise.all(
      Array.from(this.nodes.values()).map(async (node) => {
        try {
          await node.client.flushDb();
          console.log(`[ConsistentHashRing] Cleared ${node.name}`);
        } catch (error) {
          console.error(`[ConsistentHashRing] Clear error on ${node.name}:`, error);
        }
      })
    );
  }
}

module.exports = ConsistentHashRing;