'use strict';

var path = require('path');
var crypto = require('crypto');
var lmdb = require('node-lmdb/build/Release/node-lmdb');

var env = new lmdb.Env();
env.open({
  path: path.resolve(__dirname, './test-write/'),
  maxDbs: 10,
  mapSize: 268435456 * 4096,
  maxReaders: 126,
  noMetaSync: true,
  noSync: true
});

var outputSpentDbi = env.openDbi({
  name: 'outputSpent',
  create: true
});

var addressOutputsDbi = env.openDbi({
  name: 'addressOutputs',
  create: true,
  dupSort: true,
  dupFixed: true
});

var addressOutputs = {};
var outputSpents = {};
var outKeys = [];

var TOTAL_TRANSACTIONS = 1000000;
var TOTAL_INS_AND_OUTS = TOTAL_TRANSACTIONS * 2;
var TOTAL_ADDRESSES = TOTAL_TRANSACTIONS * 2 / 3;

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
  var addressValue = crypto.randomBytes(48);

  if (addressOutputs[addressKey]) {
    addressOutputs[addressKey].push(addressValue);
  } else {
    addressOutputs[addressKey] = [addressValue];
  }

  d++;
}

console.log('Starting to write to database...');

var start = new Date();

var txn = env.beginTxn();

for(var addressKey in addressOutputs) {
  var out = addressOutputs[addressKey];
  for (var i = 0; i < out.length; i++) {
    txn.putBinary(addressOutputsDbi, addressKey, out[i]);
  }
}

for(var outKey in outputSpents) {
  txn.putBinary(outputSpentDbi, outKey, outputSpents[outKey]);
}

txn.commit();
env.sync(function(err){
  if (err) {
    throw err;
  }
  var end = new Date();
  var time = (end - start) / 1000;
  console.log('Done writing ' + TOTAL_TRANSACTIONS + ' transactions in ' + time + ' seconds.');
});
