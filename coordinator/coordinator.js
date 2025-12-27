const express = require('express');
const ConsistentHashRingWithReplication = require('./ConsistentHashRingWithReplication');

const app = express();
app.use(express.json());

const parsePrimaryNodes = () => {
  const nodesStr = process.env.REDIS_NODES || 'localhost:6380,localhost:6381,localhost:6382';
  return nodesStr.split(',').map(node => {
    const [host, port] = node.trim().split(':');
    return { host, port: parseInt(port) };
  });
};

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
    console.log('[Coordinator] All Redis connections established');
    console.log('[Coordinator] Health monitoring active');
    console.log('[Coordinator] Failover manager ready');
  } catch (error) {
    console.error('[Coordinator] Failed to connect:', error);
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

// GET /health/nodes - Node health status
app.get('/health/nodes', (req, res) => {
  try {
    const status = cacheRing.healthMonitor.getAllNodeStatus();
    res.json(status);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /health/history - Health check event log
app.get('/health/history', (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const history = cacheRing.healthMonitor.getHealthHistory(limit);
    res.json({
      events: history,
      count: history.length
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /health/summary - Health summary
app.get('/health/summary', (req, res) => {
  try {
    const summary = cacheRing.healthMonitor.getHealthSummary();
    res.json(summary);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /failover/status - Current failover state
app.get('/failover/status', (req, res) => {
  try {
    const metrics = cacheRing.failoverManager.getFailoverMetrics();
    res.json(metrics);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST /failover/:nodeId/trigger - Manual failover trigger (testing)
app.post('/failover/:nodeId/trigger', async (req, res) => {
  try {
    const nodeName = `cache_node_${req.params.nodeId}`;
    const result = await cacheRing.failoverManager.failoverToReplica(nodeName);
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /system/status - Overall system health
app.get('/system/status', async (req, res) => {
  try {
    const nodeStatus = cacheRing.healthMonitor.getAllNodeStatus();
    const healthSummary = cacheRing.healthMonitor.getHealthSummary();
    const failoverMetrics = cacheRing.failoverManager.getFailoverMetrics();
    const stats = await cacheRing.getAllStats();
    
    res.json({
      overall: healthSummary.overallHealth,
      timestamp: new Date().toISOString(),
      health: {
        totalNodes: healthSummary.totalNodes,
        healthy: healthSummary.healthy,
        failed: healthSummary.failed,
        failedOver: healthSummary.failedOver,
        availability: `${((healthSummary.healthy + healthSummary.failedOver) / healthSummary.totalNodes * 100).toFixed(1)}%`
      },
      cache: {
        totalKeys: stats.totalPrimaryKeys,
        replicationHealth: stats.replicationHealth
      },
      failover: {
        totalFailovers: failoverMetrics.totalFailovers,
        successfulFailovers: failoverMetrics.successfulFailovers,
        averageFailoverTime: failoverMetrics.averageFailoverTime
      },
      nodes: nodeStatus
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

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

app.get('/health', (req, res) => {
  const healthSummary = cacheRing.healthMonitor.getHealthSummary();
  
  res.json({
    status: healthSummary.overallHealth,
    service: 'coordinator',
    algorithm: 'consistent-hashing-with-failover',
    nodes: cacheRing.nodeCount,
    replicationMode: cacheRing.replicationMode,
    virtualNodesPerNode: cacheRing.virtualNodeCount,
    totalVirtualNodes: cacheRing.sortedKeys.length,
    hashSpace: cacheRing.hashSpace,
    healthMonitoring: 'active',
    failoverCapability: 'enabled'
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Self-Healing Cache Coordinator',
    description: 'Automatic failure detection and recovery',
    version: '6.0.0',
    algorithm: 'consistent-hashing-with-automatic-failover',
    features: [
      'Automatic failure detection',
      'Read failover to replicas',
      'Automatic replica promotion',
      'Primary recovery handling',
      'Health monitoring dashboard'
    ],
    nodes: cacheRing.nodeCount,
    replicationMode: cacheRing.replicationMode,
    virtualNodesPerNode: cacheRing.virtualNodeCount,
    endpoints: {
      // Cache operations
      'POST /cache': 'Set a value',
      'GET /cache/:key': 'Get a value (with automatic failover)',
      'DELETE /cache/:key': 'Delete a key',
      'DELETE /cache': 'Clear all nodes',
      
      // Health monitoring
      'GET /health/nodes': 'Node health status',
      'GET /health/history': 'Health check event log',
      'GET /health/summary': 'Health summary',
      
      // Failover management
      'GET /failover/status': 'Failover metrics',
      'POST /failover/:nodeId/trigger': 'Manual failover (testing)',
      
      // System status
      'GET /system/status': 'Overall system health',
      
      // Replication and mapping endpoints
      'GET /stats': 'Cache statistics',
      'GET /replication/status': 'Replication status',
      'GET /replication/lag': 'Replication lag',
      'GET /distribution': 'Key distribution',
      'GET /mappings': 'Key-to-node mappings',
      'GET /ring': 'Hash ring visualization',
      'GET /health': 'Service health check'
    }
  });
});

const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log('\n' + '='.repeat(60));
  console.log('SELF-HEALING CACHE COORDINATOR STARTED');
  console.log('='.repeat(60));
  console.log(`Server: http://localhost:${PORT}`);
  console.log(`Algorithm: Consistent Hashing with Automatic Failover`);
  console.log(`Physical Redis Nodes: ${cacheRing.nodeCount}`);
  console.log(`Replication Mode: ${cacheRing.replicationMode}`);
  console.log(`Virtual Nodes per Node: ${cacheRing.virtualNodeCount}`);
  console.log(`Health Monitoring: ACTIVE`);
  console.log(`Failover Capability: ENABLED`);
  console.log('='.repeat(60) + '\n');
});

module.exports = app;