'use strict';

var path = require('path');
var crypto = require('crypto');
var lmdb = require('node-lmdb/build/Release/node-lmdb');

var env = new lmdb.Env();
env.open({
  path: path.resolve(__dirname, './testdb'),
  maxDbs: 10,
  mapSize: 268435456 * 4096,
  maxReaders: 126
});

var testDbi = env.openDbi({
  name: 'keys',
  create: true,
  dupSort: true
});

var c = 0;
while (c < 1000000) {
  var txn = env.beginTxn();
  txn.putBinary(testDbi, 'key', crypto.randomBytes(32));
  txn.commit();
  c++;
}

setInterval(function() {
  console.log('c', c);
}, 10000);

testDbi.close();
env.close();
