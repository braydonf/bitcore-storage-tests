'use strict';

var assert = require('assert');
var path = require('path');
var crypto = require('crypto');
var async = require('async');
var lmdb = require('node-lmdb/build/Release/node-lmdb');

var TRANSACTIONS_PER_ADDRESS = 2;
var TOTAL_ADDRESSES = 10000;
var TOTAL_TRANSACTIONS = TOTAL_ADDRESSES * TRANSACTIONS_PER_ADDRESS;

var addressKeys = [];
console.log('Generating random test data...');

var c = 0;
while(c < TOTAL_ADDRESSES) {
  addressKeys.push(crypto.randomBytes(21).toString('binary'));
  c++;
}

var operations = [];

var d = 0;
while(d < TOTAL_TRANSACTIONS) {
  operations.push({
    key: addressKeys[d % TOTAL_ADDRESSES],
    value: crypto.randomBytes(36)
  });
  d++;
}

var env = new lmdb.Env();
env.open({
  path: path.resolve(__dirname, './multi-txids'),
  maxDbs: 10,
  mapSize: 268435456 * 4096,
  maxReaders: 126
});

var addressDbi = env.openDbi({
  name: 'addressTxids',
  create: true,
  dupSort: true,
  dupFixed: true
});

var txn = env.beginTxn();
for (var i = 0; i < operations.length; i++) {
  txn.putBinary(addressDbi, operations[i].key, operations[i].value);
}
txn.commit();

var start = new Date();

txn = env.beginTxn();
var cursor = new lmdb.Cursor(txn, addressDbi);
var txids = [];
async.eachSeries(addressKeys, function(address, next) {
  var matches = true;
  cursor.goToKey(address);
  async.doWhilst(function(done) {
    cursor.getCurrentBinary(function(key, value) {
      matches = (address === key);
      if (matches) {
        var txid = value.slice(0, 32).toString('hex');
        var height = value.readUInt32BE(32);
        txids.push([height, txid]);
      }
      setImmediate(done);
    });
  }, function() {
    if (matches) {
      cursor.goToNext();
    }
    return matches;
  }, next);
}, function() {

  txids.sort(function(a, b) {
    return a[0] - [b][0];
  });

  assert.equal(txids.length, TOTAL_TRANSACTIONS);

  var end = new Date();
  console.log('Done reading txids for ' + TOTAL_ADDRESSES + ' in ' + (end - start) + ' milliseconds.');

});
