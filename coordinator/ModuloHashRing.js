const redis = require('redis');

class ModuloHashRing {
  constructor(redisNodes) {
    this.nodes = [];
    this.clients = [];
    this.nodeCount = redisNodes.length;
    
    console.log(`[ModuloHashRing] Initializing with ${this.nodeCount} Redis nodes`);
    
    // Create Redis clients for each node
    redisNodes.forEach((nodeConfig, index) => {
      const client = redis.createClient({
        socket: {
          host: nodeConfig.host,
          port: nodeConfig.port,
          reconnectStrategy: (retries) => {
            return Math.min(retries * 50, 500); // exponential backoff
          }
        },
        maxRetriesPerRequest: 3  // max retries for individual commands
      });

      client.on('error', (err) => {
        console.error(`[Redis Node ${index}] Error:`, err);
      });

      client.on('connect', () => {
        console.log(`[Redis Node ${index}] Connected to ${nodeConfig.host}:${nodeConfig.port}`);
      });

      this.clients.push(client);
      this.nodes.push({
        id: index,
        name: `cache_node_${index}`,
        host: nodeConfig.host,
        port: nodeConfig.port,
        client: client
      });
    });
  }

  /**
   * Connect all Redis clients
   */
  async connect() {
    console.log('[ModuloHashRing] Connecting to all Redis nodes...');
    await Promise.all(this.clients.map(client => client.connect()));
    console.log('[ModuloHashRing] All Redis nodes connected');
  }

  /**
   * Disconnect all Redis clients
   */
  async disconnect() {
    console.log('[ModuloHashRing] Disconnecting from all Redis nodes...');
    await Promise.all(this.clients.map(client => client.quit()));
    console.log('[ModuloHashRing] All Redis nodes disconnected');
  }

  /**
   * Hash function: converts a key into a number
   */
  hashFunction(key) {
    let hash = 0;
    
    for (let i = 0; i < key.length; i++) {
      hash = ((hash << 5) - hash) + key.charCodeAt(i);
      hash = hash & hash; // Convert to 32-bit integer
    }
    
    return Math.abs(hash);
  }

  /**
   * Get the node index for a given key
   */
  getNodeIndex(key) {
    const hash = this.hashFunction(key);
    return hash % this.nodeCount;
  }

  /**
   * Get the actual node object for a key
   */
  getNode(key) {
    const index = this.getNodeIndex(key);
    return this.nodes[index];
  }

  /**
   * SET operation: Store a key-value pair
   */
  async set(key, value, ttl = null) {
    const node = this.getNode(key);
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
      console.error(`[ModuloHashRing] SET error on ${node.name}:`, error);
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
    const node = this.getNode(key);
    
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
      console.error(`[ModuloHashRing] GET error on ${node.name}:`, error);
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
    const node = this.getNode(key);
    
    try {
      const result = await node.client.del(key);
      
      return {
        success: result === 1,
        node: node.name,
        nodeIndex: node.id,
        key: key
      };
    } catch (error) {
      console.error(`[ModuloHashRing] DELETE error on ${node.name}:`, error);
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
   * Get statistics for all nodes
   */
  async getAllStats() {
    const nodeStats = await Promise.all(
      this.nodes.map(async (node) => {
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
          console.error(`[ModuloHashRing] Stats error on ${node.name}:`, error);
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

    // reduce((accumulator, currentValue) => accumulator + currentValue, initial_value);
    const totalKeys = nodeStats.reduce((sum, s) => sum + (s.keys || 0), 0);
    const totalHits = nodeStats.reduce((sum, s) => sum + (s.hits || 0), 0);
    const totalMisses = nodeStats.reduce((sum, s) => sum + (s.misses || 0), 0);
    const overallHitRate = totalHits + totalMisses > 0
      ? ((totalHits / (totalHits + totalMisses)) * 100).toFixed(2)
      : '0.00';

    return {
      nodeCount: this.nodeCount,
      totalKeys: totalKeys,
      overallHitRate: `${overallHitRate}%`,
      nodes: nodeStats
    };
  }


  /**
   * Get distribution of keys across nodes
   */
  async getDistribution() {
    const stats = await this.getAllStats();

    const totalKeys = stats.totalKeys;

    return stats.nodes.map(node => ({
      nodeId: node.nodeId,
      nodeName: node.nodeName,
      keyCount: node.keys || 0,
      percentage: totalKeys > 0
        ? ((node.keys || 0) / totalKeys * 100).toFixed(2)
        : '0.00'
    }));
  }


  /**
   * Get mapping of all keys to their nodes. Modern, safe, non-blocking version 
   * async getKeyMappings() {
      const mappings = new Map();

      await Promise.all(
        this.nodes.map(async (node) => {
          try {
            for await (const key of node.client.scanIterator({
              MATCH: '*',
              COUNT: 100
            })) {
              mappings.set(key, {
                nodeIndex: node.id,
                nodeName: node.name
              });
            }
          } catch (error) {
            console.error(`[ModuloHashRing] Keys error on ${node.name}:`, error);
          }
        })
      );

      return mappings;
    }
   */

  async getKeyMappings() {
    const mappings = new Map();
    
    await Promise.all(
      this.nodes.map(async (node) => {
        try {
          const keys = await node.client.keys('*');
          keys.forEach(key => {
            mappings.set(key, {
              nodeIndex: node.id,
              nodeName: node.name
            });
          });
        } catch (error) {
          console.error(`[ModuloHashRing] Keys error on ${node.name}:`, error);
        }
      })
    );

    return mappings;
  }

  /**
   * Clear all nodes
   * Deletes all keys in the current DB using flushDb() .
   */
  async clearAll() {
    await Promise.all(
      this.nodes.map(async (node) => {
        try {
          await node.client.flushDb('ASYNC');
          console.log(`[ModuloHashRing] Cleared ${node.name}`);
        } catch (error) {
          console.error(`[ModuloHashRing] Clear error on ${node.name}:`, error);
        }
      })
    );
  }
}

module.exports = ModuloHashRing;