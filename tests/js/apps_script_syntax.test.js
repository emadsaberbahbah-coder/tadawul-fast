/* Compile every versioned Apps Script source without executing service calls.
 * Missing bound-project files require an owner export; their code is not guessed. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const sourceDir = path.join(__dirname, '../../apps_script');
const sources = fs.readdirSync(sourceDir).filter(name => name.endsWith('.gs')).sort();
assert(sources.length > 0, 'Expected versioned Apps Script sources');
for (const name of sources) {
  const source = fs.readFileSync(path.join(sourceDir, name), 'utf8');
  assert(source.trim(), name + ' must not be empty');
  new vm.Script(source, {filename: name});
  console.log('PASS Apps Script syntax ' + name);
}
console.log('Apps Script syntax: ' + sources.length + ' sources compiled');
