'use strict';

// Requires a lot of memory to hold all of the test data,
// it's possible to use --max-old-space-size=8192 flag to increase
// the amount available.

var path = require('path');
var crypto = require('crypto');
var levelup = require('levelup');

var db = levelup('./test-write');

var addressOutputs = {};
var outputSpents = {};
var outKeys = [];

var SPENT_PREFIX = new Buffer('01', 'hex');
var OUTPUTS_PREFIX = new Buffer('02', 'hex');

var TOTAL_TRANSACTIONS = 1000000;
var TOTAL_INS_AND_OUTS = TOTAL_TRANSACTIONS * 2; // Two inputs and outputs per transaction
var TOTAL_ADDRESSES = TOTAL_TRANSACTIONS / 2; // Two transactions per address

console.log('Generating random test data...');

var c = 0;
while (c < TOTAL_ADDRESSES) {
  outKeys.push(crypto.randomBytes(21).toString('binary'));
  c++;
}

var d = 0;
while (d < TOTAL_INS_AND_OUTS) {

  var key = crypto.randomBytes(36).toString('binary');
  var value = crypto.randomBytes(25);

  outputSpents[key] = value;

  var addressKey = outKeys[d % TOTAL_ADDRESSES];

  var addressKeyPlus = crypto.randomBytes(36);

  var addressValue = crypto.randomBytes(12);

  if (addressOutputs[addressKey]) {
    addressOutputs[addressKey].push([addressKeyPlus, addressValue]);
  } else {
    addressOutputs[addressKey] = [[addressKeyPlus, addressValue]];
  }

  d++;
}

console.log('Creating batch operations...');

var operations = [];

for(var addressKey in addressOutputs) {
  var out = addressOutputs[addressKey];
  for (var i = 0; i < out.length; i++) {
    operations.push({
      type: 'put',
      key: Buffer.concat([SPENT_PREFIX, new Buffer(addressKey, 'binary'), out[i][0]]),
      value: out[i][1]
    });
  }
  delete addressOutputs[addressKey];
}

for(var outKey in outputSpents) {
  operations.push({
    type: 'put',
    key: Buffer.concat([OUTPUTS_PREFIX, new Buffer(outKey, 'binary')]),
    value: outputSpents[outKey]
  });
  delete outputSpents[outKey];
}

console.log('Starting to write...');

var start = new Date();

db.batch(operations, function(err) {
  if (err) {
    throw err;
  }
  var end = new Date();
  var time = (end - start) / 1000;
  console.log('Done writing ' + TOTAL_TRANSACTIONS + ' transactions in ' + time + ' seconds.');
});
