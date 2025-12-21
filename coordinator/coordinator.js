const express = require('express');
const ConsistentHashRingWithReplication = require('./ConsistentHashRingWithReplication');

const app = express();
app.use(express.json());

// Parse primary Redis nodes
const parsePrimaryNodes = () => {
  const nodesStr = process.env.REDIS_NODES || 'localhost:6380,localhost:6381,localhost:6382';
  return nodesStr.split(',').map(node => {
    const [host, port] = node.trim().split(':');
    return { host, port: parseInt(port) };
  });
};

// Parse replica Redis nodes
const parseReplicaNodes = () => {
  const nodesStr = process.env.REDIS_REPLICAS || 'localhost:6390,localhost:6391,localhost:6392';
  return nodesStr.split(',').map(node => {
    const [host, port] = node.trim().split(':');
    return { host, port: parseInt(port) };
  });
};

const primaryNodes = parsePrimaryNodes();
const replicaNodes = parseReplicaNodes();
const virtualNodeCount = parseInt(process.env.VIRTUAL_NODES || '150');
const replicationMode = process.env.REPLICATION_MODE || 'async';

const cacheRing = new ConsistentHashRingWithReplication(
  primaryNodes,
  replicaNodes,
  virtualNodeCount,
  replicationMode
);

// Initialize Redis connections
(async () => {
  try {
    await cacheRing.connect();
    console.log('[Coordinator] All Redis connections established (primaries + replicas)');
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
  const { key, value, ttl, replicationMode } = req.body;

  if (!key || value === undefined) {
    return res.status(400).json({
      success: false,
      error: 'Key and value are required'
    });
  }

  try {
    const result = await cacheRing.set(key, value, ttl, replicationMode);
    
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

// GET /replication/status
app.get('/replication/status', async (req, res) => {
  try {
    const stats = await cacheRing.getAllStats();
    
    res.json({
      overall: stats.replicationHealth,
      mode: stats.replicationMode,
      primaryKeys: stats.totalPrimaryKeys,
      replicaKeys: stats.totalReplicaKeys,
      keysInSync: stats.totalPrimaryKeys === stats.totalReplicaKeys,
      nodes: stats.nodes.map(n => ({
        name: n.nodeName,
        primary: {
          keys: n.primary?.keys || 0,
          host: n.primary?.host
        },
        replica: {
          keys: n.replica?.keys || 0,
          host: n.replica?.host
        },
        replication: n.replication
      }))
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /replication/lag
app.get('/replication/lag', async (req, res) => {
  try {
    const lagInfo = await cacheRing.getReplicationLag();
    res.json(lagInfo);
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
      replicationMode: cacheRing.replicationMode,
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
      replicationMode: cacheRing.replicationMode,
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
    algorithm: 'consistent-hashing-with-replication',
    nodes: cacheRing.nodeCount,
    replicationMode: cacheRing.replicationMode,
    virtualNodesPerNode: cacheRing.virtualNodeCount,
    totalVirtualNodes: cacheRing.sortedKeys.length,
    hashSpace: cacheRing.hashSpace
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Replication-Aware Cache Coordinator',
    description: 'Lab 4: Master-replica replication with CAP theorem',
    version: '4.0.0',
    algorithm: 'consistent-hashing-with-replication',
    nodes: cacheRing.nodeCount,
    replicationMode: cacheRing.replicationMode,
    virtualNodesPerNode: cacheRing.virtualNodeCount,
    totalVirtualNodes: cacheRing.sortedKeys.length,
    hashSpace: cacheRing.hashSpace,
    endpoints: {
      'POST /cache': 'Set a value (body: { key, value, ttl?, replicationMode? })',
      'GET /cache/:key': 'Get a value',
      'DELETE /cache/:key': 'Delete a key',
      'DELETE /cache': 'Clear all nodes',
      'GET /stats': 'Get cache statistics with replication info',
      'GET /replication/status': 'Get replication status',
      'GET /replication/lag': 'Get replication lag details',
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
  console.log('REPLICATION-AWARE COORDINATOR STARTED');
  console.log('='.repeat(60));
  console.log(`Server: http://localhost:${PORT}`);
  console.log(`Algorithm: Consistent Hashing with Replication`);
  console.log(`Physical Redis Nodes: ${cacheRing.nodeCount}`);
  console.log(`Replication Mode: ${cacheRing.replicationMode}`);
  console.log(`Virtual Nodes per Node: ${cacheRing.virtualNodeCount}`);
  console.log(`Total Virtual Nodes: ${cacheRing.sortedKeys.length}`);
  console.log(`Hash Space: 0 to ${cacheRing.hashSpace - 1}`);
  console.log('='.repeat(60) + '\n');
});

module.exports = app;