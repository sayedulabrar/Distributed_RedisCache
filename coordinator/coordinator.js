const express = require('express');
const ModuloHashRing = require('./ModuloHashRing');

const app = express();
app.use(express.json());

// Parse Redis nodes from environment variable
const parseRedisNodes = () => {
  const nodesStr = process.env.REDIS_NODES || 'localhost:6380,localhost:6381,localhost:6382'; //we pass the environment var in docker compose
  return nodesStr.split(',').map(node => {
    const [host, port] = node.trim().split(':');
    return { host, port: parseInt(port) };
  });
};

const redisNodes = parseRedisNodes();
const cacheRing = new ModuloHashRing(redisNodes);

// Initialize Redis connections
(async () => {
  try {
    await cacheRing.connect();
    console.log('[Coordinator] All Redis connections established');
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

// POST /hash
app.post('/hash', (req, res) => {
  const { key } = req.body;

  if (!key) {
    return res.status(400).json({
      success: false,
      error: 'Key is required'
    });
  }

  const hash = cacheRing.hashFunction(key);
  const nodeIndex = cacheRing.getNodeIndex(key);

  res.json({
    key: key,
    hash: hash,
    nodeIndex: nodeIndex,
    nodeName: `cache_node_${nodeIndex}`,
    calculation: `${hash} % ${cacheRing.nodeCount} = ${nodeIndex}`
  });
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
    nodes: cacheRing.nodeCount
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Cache Coordinator Service',
    description: 'Modulo hashing coordinator for distributed Redis cache',
    version: '1.0.0',
    nodes: cacheRing.nodeCount,
    endpoints: {
      'POST /cache': 'Set a value (body: { key, value, ttl? })',
      'GET /cache/:key': 'Get a value',
      'DELETE /cache/:key': 'Delete a key',
      'DELETE /cache': 'Clear all nodes',
      'GET /stats': 'Get cache statistics',
      'GET /distribution': 'See key distribution',
      'GET /mappings': 'Get all key-to-node mappings',
      'POST /hash': 'Get hash for a key (body: { key })',
      'GET /health': 'Health check'
    }
  });
});

// Start server
const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log('\n' + '='.repeat(60));
  console.log('COORDINATOR SERVICE STARTED');
  console.log('='.repeat(60));
  console.log(`Server: http://localhost:${PORT}`);
  console.log(`Redis Nodes: ${cacheRing.nodeCount}`);
  console.log('='.repeat(60) + '\n');
});

module.exports = app;