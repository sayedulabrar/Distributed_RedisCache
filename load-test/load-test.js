const axios = require('axios');

const API_URL = process.env.API_URL || 'http://localhost';
const DURATION_SECONDS = parseInt(process.env.DURATION) || 60;
const CONCURRENCY = parseInt(process.env.CONCURRENCY) || 10;
const KEY_SPACE = parseInt(process.env.KEY_SPACE) || 1000;

// Statistics
const stats = {
  requests: {
    total: 0,
    successful: 0,
    failed: 0,
    get: 0,
    post: 0,
    delete: 0
  },
  latencies: [],
  errors: new Map(),
  servers: new Map(),
  startTime: Date.now()
};

// Zipfian distribution for hot keys
function zipfianKey() {
  const rand = Math.random();
  
  // 20% of keys get 80% of traffic (Pareto principle)
  if (rand < 0.8) {
    return `key_${Math.floor(Math.random() * (KEY_SPACE * 0.2))}`;
  } else {
    return `key_${Math.floor(Math.random() * KEY_SPACE)}`;
  }
}

// Worker function
async function worker(workerId) {
  const endTime = Date.now() + (DURATION_SECONDS * 1000);
  
  console.log(`[Worker ${workerId}] Started`);
  
  while (Date.now() < endTime) {
    const startTime = Date.now();
    
    try {
      const operation = Math.random();
      let response;
      
      if (operation < 0.70) {
        // 70% GET requests
        const key = zipfianKey();
        response = await axios.get(`${API_URL}/cache/${key}`);
        stats.requests.get++;
      } else if (operation < 0.95) {
        // 25% POST requests
        const key = zipfianKey();
        const value = {
          data: `test_value_${Math.random()}`,
          timestamp: Date.now(),
          worker: workerId
        };
        response = await axios.post(`${API_URL}/cache`, {
          key,
          value,
          ttl: 300
        });
        stats.requests.post++;
      } else {
        // 5% DELETE requests
        const key = zipfianKey();
        response = await axios.delete(`${API_URL}/cache/${key}`);
        stats.requests.delete++;
      }
      
      const latency = Date.now() - startTime;
      
      stats.requests.total++;
      stats.requests.successful++;
      stats.latencies.push(latency);
      
      // Track which server handled the request
      const serverId = response.headers['x-server-id'] || 'unknown';
      stats.servers.set(serverId, (stats.servers.get(serverId) || 0) + 1);
      
    } catch (error) {
      stats.requests.total++;
      stats.requests.failed++;
      
      const errorType = error.response?.status || error.code || 'unknown';
      stats.errors.set(errorType, (stats.errors.get(errorType) || 0) + 1);
    }
    
    // Small delay to avoid overwhelming the system
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  
  console.log(`[Worker ${workerId}] Finished`);
}

// Calculate percentile
function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, index)];
}

// Print results
function printResults() {
  const duration = (Date.now() - stats.startTime) / 1000;
  const rps = stats.requests.total / duration;
  
  console.log('\n' + '='.repeat(70));
  console.log('LOAD TEST RESULTS');
  console.log('='.repeat(70));
  
  console.log('\n📊 OVERALL STATISTICS:');
  console.log(`  Duration:        ${duration.toFixed(2)}s`);
  console.log(`  Total Requests:  ${stats.requests.total.toLocaleString()}`);
  console.log(`  Successful:      ${stats.requests.successful.toLocaleString()} (${((stats.requests.successful / stats.requests.total) * 100).toFixed(2)}%)`);
  console.log(`  Failed:          ${stats.requests.failed.toLocaleString()} (${((stats.requests.failed / stats.requests.total) * 100).toFixed(2)}%)`);
  console.log(`  Throughput:      ${rps.toFixed(2)} req/s`);
  
  console.log('\n📈 REQUEST TYPES:');
  console.log(`  GET:             ${stats.requests.get.toLocaleString()} (${((stats.requests.get / stats.requests.total) * 100).toFixed(2)}%)`);
  console.log(`  POST:            ${stats.requests.post.toLocaleString()} (${((stats.requests.post / stats.requests.total) * 100).toFixed(2)}%)`);
  console.log(`  DELETE:          ${stats.requests.delete.toLocaleString()} (${((stats.requests.delete / stats.requests.total) * 100).toFixed(2)}%)`);
  
  console.log('\n⏱️  LATENCY (milliseconds):');
  if (stats.latencies.length > 0) {
    const avg = stats.latencies.reduce((a, b) => a + b, 0) / stats.latencies.length;
    const min = Math.min(...stats.latencies);
    const max = Math.max(...stats.latencies);
    
    console.log(`  Min:             ${min}ms`);
    console.log(`  Average:         ${avg.toFixed(2)}ms`);
    console.log(`  P50 (median):    ${percentile(stats.latencies, 50)}ms`);
    console.log(`  P95:             ${percentile(stats.latencies, 95)}ms`);
    console.log(`  P99:             ${percentile(stats.latencies, 99)}ms`);
    console.log(`  Max:             ${max}ms`);
  }
  
  console.log('\n🖥️  SERVER DISTRIBUTION:');
  const sortedServers = Array.from(stats.servers.entries()).sort((a, b) => b[1] - a[1]);
  sortedServers.forEach(([serverId, count]) => {
    const percentage = (count / stats.requests.total * 100).toFixed(2);
    console.log(`  Server ${serverId}:       ${count.toLocaleString()} requests (${percentage}%)`);
  });
  
  if (stats.errors.size > 0) {
    console.log('\n❌ ERRORS:');
    const sortedErrors = Array.from(stats.errors.entries()).sort((a, b) => b[1] - a[1]);
    sortedErrors.forEach(([errorType, count]) => {
      console.log(`  ${errorType}:            ${count.toLocaleString()}`);
    });
  }
  
  console.log('\n' + '='.repeat(70) + '\n');
}

// Main function
async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('DISTRIBUTED CACHE LOAD TEST');
  console.log('='.repeat(70));
  console.log(`\n🎯 Configuration:`);
  console.log(`  API URL:         ${API_URL}`);
  console.log(`  Duration:        ${DURATION_SECONDS}s`);
  console.log(`  Concurrency:     ${CONCURRENCY} workers`);
  console.log(`  Key Space:       ${KEY_SPACE} keys`);
  console.log(`  Distribution:    70% GET, 25% POST, 5% DELETE`);
  console.log(`  Hot Keys:        20% of keys receive 80% of traffic`);
  console.log('\n' + '='.repeat(70) + '\n');
  
  // Pre-populate cache with some data
  console.log('📝 Pre-populating cache...');
  for (let i = 0; i < Math.min(100, KEY_SPACE); i++) {
    try {
      await axios.post(`${API_URL}/cache`, {
        key: `key_${i}`,
        value: { data: `initial_value_${i}` },
        ttl: 300
      });
    } catch (error) {
      // Ignore errors during pre-population
    }
  }
  console.log('✅ Pre-population complete\n');
  
  // Start workers
  console.log(`🚀 Starting ${CONCURRENCY} workers...\n`);
  stats.startTime = Date.now();
  
  const workers = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workers.push(worker(i + 1));
  }
  
  await Promise.all(workers);
  
  printResults();
}

// Run the test
main().catch(error => {
  console.error('Load test failed:', error);
  process.exit(1);
});