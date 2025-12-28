const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const COORDINATOR_URL = process.env.COORDINATOR_URL || 'http://localhost:3001';
const SERVER_ID = process.env.SERVER_ID || '1';
const PORT = process.env.PORT || 4000;

// Metrics storage
const metrics = {
  requests: {
    total: 0,
    successful: 0,
    failed: 0
  },
  cache: {
    hits: 0,
    misses: 0,
    sets: 0,
    deletes: 0
  },
  latencies: [], // Store last 10000 latencies
  startTime: Date.now(),
  serverId: SERVER_ID
};

// Local in-memory cache for frequently accessed keys
const localCache = new Map();
const LOCAL_CACHE_TTL = 5000; // 5 seconds
const LOCAL_CACHE_MAX_SIZE = 1000;

// Axios instance with connection pooling
const coordinatorClient = axios.create({
  baseURL: COORDINATOR_URL,
  timeout: 5000,
  maxRedirects: 0,
  httpAgent: new (require('http').Agent)({ 
    keepAlive: true,
    maxSockets: 50
  })
});

// Middleware: Request timing and logging
app.use((req, res, next) => {
  const startTime = Date.now();
  
  // Log request
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} - Server ${SERVER_ID}`);
  
  // Capture response
  const originalSend = res.send;
  res.send = function(data) {
    const duration = Date.now() - startTime;
    
    // Store latency (keep last 10000)
    if (metrics.latencies.length >= 10000) {
      metrics.latencies.shift();
    }
    metrics.latencies.push(duration);
    
    // Update request metrics
    metrics.requests.total++;
    if (res.statusCode >= 200 && res.statusCode < 400) {
      metrics.requests.successful++;
    } else {
      metrics.requests.failed++;
    }
    
    // Add performance headers
    res.setHeader('X-Response-Time', `${duration}ms`);
    res.setHeader('X-Server-ID', SERVER_ID);
    
    originalSend.call(this, data);
  };
  
  next();
});

// Helper: Calculate percentiles
function calculatePercentile(arr, percentile) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.ceil((percentile / 100) * sorted.length) - 1;
  return sorted[index];
}

// Helper: Check local cache
function getFromLocalCache(key) {
  const cached = localCache.get(key);
  if (!cached) return null;
  
  // Check if expired
  if (Date.now() - cached.timestamp > LOCAL_CACHE_TTL) {
    localCache.delete(key);
    return null;
  }
  
  return cached.value;
}

// Helper: Set local cache
function setToLocalCache(key, value) {
  // Implement LRU eviction if cache is full
  if (localCache.size >= LOCAL_CACHE_MAX_SIZE) {
    const firstKey = localCache.keys().next().value;
    localCache.delete(firstKey);
  }
  
  localCache.set(key, {
    value: value,
    timestamp: Date.now()
  });
}

// POST /cache - Set a cache value
app.post('/cache', async (req, res) => {
  try {
    const { key, value, ttl } = req.body;
    
    if (!key || value === undefined) {
      return res.status(400).json({
        success: false,
        error: 'Key and value are required'
      });
    }
    
    // Forward to coordinator
    const response = await coordinatorClient.post('/cache', {
      key,
      value,
      ttl
    });
    
    // Update metrics
    metrics.cache.sets++;
    
    // Update local cache
    setToLocalCache(key, value);
    
    res.status(201).json({
      ...response.data,
      serverId: SERVER_ID
    });
  } catch (error) {
    console.error(`[Server ${SERVER_ID}] SET error:`, error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      serverId: SERVER_ID
    });
  }
});

// GET /cache/:key - Get a cache value
app.get('/cache/:key', async (req, res) => {
  try {
    const { key } = req.params;
    
    // Check local cache first
    const localValue = getFromLocalCache(key);
    if (localValue !== null) {
      metrics.cache.hits++;
      return res.json({
        success: true,
        key: key,
        value: localValue,
        origin: 'local-cache',
        serverId: SERVER_ID
      });
    }
    
    // Forward to coordinator
    const response = await coordinatorClient.get(`/cache/${key}`);
    
    if (response.data.success) {
      metrics.cache.hits++;
      
      // Store in local cache
      setToLocalCache(key, response.data.value);
      
      res.json({
        ...response.data,
        origin: 'redis',
        serverId: SERVER_ID
      });
    } else {
      metrics.cache.misses++;
      res.status(404).json({
        ...response.data,
        serverId: SERVER_ID
      });
    }
  } catch (error) {
    metrics.cache.misses++;
    console.error(`[Server ${SERVER_ID}] GET error:`, error.message);
    
    if (error.response && error.response.status === 404) {
      res.status(404).json({
        success: false,
        error: 'Key not found',
        serverId: SERVER_ID
      });
    } else {
      res.status(500).json({
        success: false,
        error: error.message,
        serverId: SERVER_ID
      });
    }
  }
});

// DELETE /cache/:key - Delete a cache value
app.delete('/cache/:key', async (req, res) => {
  try {
    const { key } = req.params;
    
    // Remove from local cache
    localCache.delete(key);
    
    // Forward to coordinator
    const response = await coordinatorClient.delete(`/cache/${key}`);
    
    metrics.cache.deletes++;
    
    res.json({
      ...response.data,
      serverId: SERVER_ID
    });
  } catch (error) {
    console.error(`[Server ${SERVER_ID}] DELETE error:`, error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      serverId: SERVER_ID
    });
  }
});

// GET /metrics - Get server metrics
app.get('/metrics', async (req, res) => {
  try {
    // Calculate latency percentiles
    const p50 = calculatePercentile(metrics.latencies, 50);
    const p95 = calculatePercentile(metrics.latencies, 95);
    const p99 = calculatePercentile(metrics.latencies, 99);
    const avg = metrics.latencies.length > 0
      ? metrics.latencies.reduce((a, b) => a + b, 0) / metrics.latencies.length
      : 0;
    
    // Calculate hit rate
    const totalCacheOps = metrics.cache.hits + metrics.cache.misses;
    const hitRate = totalCacheOps > 0
      ? (metrics.cache.hits / totalCacheOps * 100).toFixed(2)
      : '0.00';
    
    // Calculate uptime
    const uptimeSeconds = Math.floor((Date.now() - metrics.startTime) / 1000);
    
    // Calculate requests per second
    const rps = uptimeSeconds > 0
      ? (metrics.requests.total / uptimeSeconds).toFixed(2)
      : '0.00';
    
    res.json({
      serverId: SERVER_ID,
      uptime: {
        seconds: uptimeSeconds,
        formatted: `${Math.floor(uptimeSeconds / 3600)}h ${Math.floor((uptimeSeconds % 3600) / 60)}m ${uptimeSeconds % 60}s`
      },
      requests: {
        ...metrics.requests,
        requestsPerSecond: rps,
        successRate: metrics.requests.total > 0
          ? ((metrics.requests.successful / metrics.requests.total) * 100).toFixed(2) + '%'
          : '0.00%'
      },
      cache: {
        ...metrics.cache,
        hitRate: hitRate + '%',
        localCacheSize: localCache.size,
        localCacheMaxSize: LOCAL_CACHE_MAX_SIZE
      },
      latency: {
        average: avg.toFixed(2) + 'ms',
        p50: p50 + 'ms',
        p95: p95 + 'ms',
        p99: p99 + 'ms',
        samples: metrics.latencies.length
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      serverId: SERVER_ID
    });
  }
});

// GET /health - Health check
app.get('/health', async (req, res) => {
  try {
    // Check coordinator health
    const coordinatorHealth = await coordinatorClient.get('/health');
    
    res.json({
      status: 'healthy',
      serverId: SERVER_ID,
      uptime: Math.floor((Date.now() - metrics.startTime) / 1000),
      coordinator: coordinatorHealth.data,
      localCache: {
        size: localCache.size,
        maxSize: LOCAL_CACHE_MAX_SIZE
      }
    });
  } catch (error) {
    res.status(503).json({
      status: 'unhealthy',
      serverId: SERVER_ID,
      error: error.message
    });
  }
});

// GET / - API info
app.get('/', (req, res) => {
  res.json({
    name: 'Distributed Cache API Server',
    version: '5.0.0',
    serverId: SERVER_ID,
    features: [
      'Load balancing support',
      'Local caching layer',
      'Metrics collection',
      'Connection pooling',
      'Graceful shutdown'
    ],
    endpoints: {
      'POST /cache': 'Set a cache value (body: { key, value, ttl? })',
      'GET /cache/:key': 'Get a cache value',
      'DELETE /cache/:key': 'Delete a cache value',
      'GET /metrics': 'Get server metrics',
      'GET /health': 'Health check'
    }
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log(`[Server ${SERVER_ID}] Received SIGTERM, shutting down gracefully...`);
  
  server.close(() => {
    console.log(`[Server ${SERVER_ID}] HTTP server closed`);
    process.exit(0);
  });
  
  // Force shutdown after 10 seconds
  setTimeout(() => {
    console.error(`[Server ${SERVER_ID}] Forced shutdown`);
    process.exit(1);
  }, 10000);
});

// Start server
const server = app.listen(PORT, () => {
  console.log('\n' + '='.repeat(60));
  console.log(`API SERVER ${SERVER_ID} STARTED`);
  console.log('='.repeat(60));
  console.log(`Server: http://localhost:${PORT}`);
  console.log(`Coordinator: ${COORDINATOR_URL}`);
  console.log(`Local Cache: ${LOCAL_CACHE_MAX_SIZE} items, ${LOCAL_CACHE_TTL}ms TTL`);
  console.log('='.repeat(60) + '\n');
});

module.exports = app;