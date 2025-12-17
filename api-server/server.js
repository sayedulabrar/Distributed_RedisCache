const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const COORDINATOR_URL = process.env.COORDINATOR_URL || 'http://localhost:3001';

// Request logging
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// Forward requests to coordinator
const forwardToCoordinator = async (method, path, data = null) => {
  try {
    const config = {
      method: method,
      url: `${COORDINATOR_URL}${path}`,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    if (data) {
      config.data = data;
    }

    const response = await axios(config);
    return { success: true, data: response.data };
  } catch (error) {
    console.error(`[API Server] Error forwarding to coordinator:`, error.message);
    return {
      success: false,
      error: error.response?.data || error.message,
      status: error.response?.status || 500
    };
  }
};

// POST /cache
app.post('/cache', async (req, res) => {
  const result = await forwardToCoordinator('POST', '/cache', req.body);
  
  if (result.success) {
    res.status(201).json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// GET /cache/:key
app.get('/cache/:key', async (req, res) => {
  const result = await forwardToCoordinator('GET', `/cache/${req.params.key}`);
  
  if (result.success) {
    res.json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// DELETE /cache/:key
app.delete('/cache/:key', async (req, res) => {
  const result = await forwardToCoordinator('DELETE', `/cache/${req.params.key}`);
  
  if (result.success) {
    res.json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// GET /stats
app.get('/stats', async (req, res) => {
  const result = await forwardToCoordinator('GET', '/stats');
  
  if (result.success) {
    res.json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// GET /distribution
app.get('/distribution', async (req, res) => {
  const result = await forwardToCoordinator('GET', '/distribution');
  
  if (result.success) {
    res.json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// GET /mappings
app.get('/mappings', async (req, res) => {
  const result = await forwardToCoordinator('GET', '/mappings');
  
  if (result.success) {
    res.json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// GET /ring (New endpoint for Lab 2)
app.get('/ring', async (req, res) => {
  const result = await forwardToCoordinator('GET', '/ring');
  
  if (result.success) {
    res.json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// DELETE /cache
app.delete('/cache', async (req, res) => {
  const result = await forwardToCoordinator('DELETE', '/cache');
  
  if (result.success) {
    res.json(result.data);
  } else {
    res.status(result.status).json(result.error);
  }
});

// Health check
app.get('/health', async (req, res) => {
  const coordHealth = await forwardToCoordinator('GET', '/health');
  
  res.json({
    status: 'healthy',
    service: 'api-server',
    coordinator: coordHealth.success ? coordHealth.data : 'unavailable'
  });
});

app.get('/', async (req, res) => {
  const coordInfo = await forwardToCoordinator('GET', '/');
  
  res.json({
    name: 'Consistent Hashing Cache API',
    description: 'Lab 2: Solving the scale-up problem with consistent hashing',
    version: '2.0.0',
    coordinator: coordInfo.success ? coordInfo.data : 'unavailable',
    endpoints: {
      'POST /cache': 'Set a value (body: { key, value, ttl? })',
      'GET /cache/:key': 'Get a value',
      'DELETE /cache/:key': 'Delete a key',
      'DELETE /cache': 'Clear all nodes',
      'GET /stats': 'Get cache statistics',
      'GET /distribution': 'See key distribution',
      'GET /mappings': 'Get all key-to-node mappings',
      'GET /ring': 'Visualize the hash ring',
      'GET /health': 'Health check'
    }
  });
});

// Start server
const PORT = process.env.PORT || 4000;

app.listen(PORT, () => {
  console.log('\n' + '='.repeat(60));
  console.log('API SERVER STARTED');
  console.log('='.repeat(60));
  console.log(`Server: http://localhost:${PORT}`);
  console.log(`Coordinator: ${COORDINATOR_URL}`);
  console.log('='.repeat(60) + '\n');
});

module.exports = app;