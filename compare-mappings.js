// compare-mappings.js
const fs = require('fs');

const old = JSON.parse(fs.readFileSync('mappings-consistent-3nodes.json'));
const newM = JSON.parse(fs.readFileSync('mappings-consistent-4nodes.json'));

let moved = 0;
let stayed = 0;

const migrations = [];

for (let key in old.mappings) {
  const oldNode = old.mappings[key].nodeIndex;
  const newNode = newM.mappings[key].nodeIndex;
  
  if (oldNode !== newNode) {
    moved++;
    migrations.push({
      key,
      from: oldNode,
      to: newNode
    });
  } else {
    stayed++;
  }
}

console.log('\n' + '='.repeat(60));
console.log('SCALE-UP ANALYSIS: 3 NODES → 4 NODES');
console.log('='.repeat(60));
console.log(`Total keys: ${moved + stayed}`);
console.log(`Keys that stayed: ${stayed} (${(stayed/(moved+stayed)*100).toFixed(1)}%)`);
console.log(`Keys that moved: ${moved} (${(moved/(moved+stayed)*100).toFixed(1)}%)`);
console.log('\nFirst 20 migrations:');
migrations.slice(0, 20).forEach(m => {
  console.log(`  ${m.key.padEnd(15)}: node_${m.from} → node_${m.to}`);
});
console.log('\n' + '='.repeat(60));