const express = require('express');
const ConsistentHashRingWithVNodes = require('./ConsistentHashRingWithVNodes');

const app = express();
app.use(express.json());

// Parse Redis nodes from environment variable
const parseRedisNodes = () => {
  const nodesStr = process.env.REDIS_NODES || 'localhost:6380,localhost:6381,localhost:6382';
  return nodesStr.split(',').map(node => {
    const [host, port] = node.trim().split(':');
    return { host, port: parseInt(port) };
  });
};

const redisNodes = parseRedisNodes();
const virtualNodeCount = parseInt(process.env.VIRTUAL_NODES || '150');
const cacheRing = new ConsistentHashRingWithVNodes(redisNodes, virtualNodeCount);

// Initialize Redis connections
(async () => {
  try {
    await cacheRing.connect();
    console.log('[Coordinator] All Redis connections established with virtual nodes');
  } catch (error) {
    console.error('[Coordinator] Failed to connect to Redis:', error);
    process.exit(1);
  }
})();

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('[Coordinator] Shutting down gracefully...');
  await cacheRing.disconnect();
  process.exit(0);
});

// Request logging
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// POST /cache
app.post('/cache', async (req, res) => {
  const { key, value, ttl } = req.body;

  if (!key || value === undefined) {
    return res.status(400).json({
      success: false,
      error: 'Key and value are required'
    });
  }

  try {
    const result = await cacheRing.set(key, value, ttl);
    
    if (result.success) {
      res.status(201).json(result);
    } else {
      res.status(500).json(result);
    }
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /cache/:key
app.get('/cache/:key', async (req, res) => {
  const { key } = req.params;

  try {
    const result = await cacheRing.get(key);

    if (result.success) {
      res.json(result);
    } else {
      res.status(404).json(result);
    }
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// DELETE /cache/:key
app.delete('/cache/:key', async (req, res) => {
  const { key } = req.params;

  try {
    const result = await cacheRing.delete(key);
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /stats
app.get('/stats', async (req, res) => {
  try {
    const stats = await cacheRing.getAllStats();
    res.json(stats);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /distribution
app.get('/distribution', async (req, res) => {
  try {
    const distribution = await cacheRing.getDistribution();
    res.json({
      totalNodes: cacheRing.nodeCount,
      virtualNodesPerNode: cacheRing.virtualNodeCount,
      distribution: distribution
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /mappings
app.get('/mappings', async (req, res) => {
  try {
    const mappings = await cacheRing.getKeyMappings();
    
    const result = {};
    mappings.forEach((value, key) => {
      result[key] = {
        node: value.nodeName,
        nodeIndex: value.nodeIndex
      };
    });

    res.json({
      totalKeys: mappings.size,
      mappings: result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /ring
app.get('/ring', (req, res) => {
  try {
    const visualization = cacheRing.visualizeRing();
    res.json({
      hashSpace: cacheRing.hashSpace,
      nodeCount: cacheRing.nodeCount,
      ...visualization
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// DELETE /cache
app.delete('/cache', async (req, res) => {
  try {
    await cacheRing.clearAll();
    res.json({
      success: true,
      message: 'All cache nodes cleared'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'coordinator',
    algorithm: 'consistent-hashing-with-virtual-nodes',
    nodes: cacheRing.nodeCount,
    virtualNodesPerNode: cacheRing.virtualNodeCount,
    totalVirtualNodes: cacheRing.sortedKeys.length,
    hashSpace: cacheRing.hashSpace
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Virtual Nodes Cache Coordinator',
    description: 'Lab 3: Perfect load distribution with virtual nodes',
    version: '3.0.0',
    algorithm: 'consistent-hashing-with-virtual-nodes',
    nodes: cacheRing.nodeCount,
    virtualNodesPerNode: cacheRing.virtualNodeCount,
    totalVirtualNodes: cacheRing.sortedKeys.length,
    hashSpace: cacheRing.hashSpace,
    endpoints: {
      'POST /cache': 'Set a value (body: { key, value, ttl? })',
      'GET /cache/:key': 'Get a value',
      'DELETE /cache/:key': 'Delete a key',
      'DELETE /cache': 'Clear all nodes',
      'GET /stats': 'Get cache statistics',
      'GET /distribution': 'See key distribution',
      'GET /mappings': 'Get all key-to-node mappings',
      'GET /ring': 'Visualize the virtual node ring',
      'GET /health': 'Health check'
    }
  });
});

// Start server
const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log('\n' + '='.repeat(60));
  console.log('VIRTUAL NODES COORDINATOR STARTED');
  console.log('='.repeat(60));
  console.log(`Server: http://localhost:${PORT}`);
  console.log(`Algorithm: Consistent Hashing with Virtual Nodes`);
  console.log(`Physical Redis Nodes: ${cacheRing.nodeCount}`);
  console.log(`Virtual Nodes per Node: ${cacheRing.virtualNodeCount}`);
  console.log(`Total Virtual Nodes: ${cacheRing.sortedKeys.length}`);
  console.log(`Hash Space: 0 to ${cacheRing.hashSpace - 1}`);
  console.log('='.repeat(60) + '\n');
});

module.exports = app;
