'use strict';

var path = require('path');
var crypto = require('crypto');
var async = require('async');
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
  dupSort: true,
  dupFixed: true
});

var spentDbi = env.openDbi({
  name: 'keys',
  create: true
});

function randomHeightBuffer() {
  var heightBuffer = new Buffer(new Array(4));
  heightBuffer.writeUInt32BE(Math.round(Math.random() * 4000));
  return heightBuffer;
}

function randomTxid() {
  return crypto.randomBytes(32);
}

function randomIndex() {
  var buf = new Buffer(new Array(4));
  buf.writeUInt32BE(Math.round(Math.random() * 100));
  return buf;
}

function randomSatoshis() {
  var buf = new Buffer(new Array(8));
  buf.writeDoubleBE(Math.round(Math.random() * 1e8));
  return buf;
}

var address = 'address';
var c = 0;
var keys = [];
var spents = [];
while (c < 3195670 * 2) {
  var outputTxid = randomTxid();
  var outputIndex = randomIndex();
  var value = Buffer.concat([randomHeightBuffer(), outputTxid, outputIndex, randomSatoshis()]);
  keys.push([address, value]);

  if (Math.round(Math.random())) {
    var spentValue = Buffer.concat([randomTxid(), randomIndex(), randomHeightBuffer()]);
    keys.push([outputTxid.toString('binary') + outputIndex.toString('binary'), spentValue]);
  }

  c++;
}

var start = new Date();
var txn = env.beginTxn();
for (var i = 0; i < keys.length; i++) {
  txn.putBinary(testDbi, keys[i][0], keys[i][1]);
}
txn.commit();
var txn = env.beginTxn();
for (var j = 0; j < keys.length; j++) {
  txn.putBinary(spentDbi, keys[j][0], keys[j][1]);
}
txn.commit();
var end = new Date();

console.log('Done writing in ' + (end - start) + ' milliseconds.');


var start = new Date();
var txn = env.beginTxn();
var cursor = new lmdb.Cursor(txn, testDbi);
var outputs = [];
var matches = true;
cursor.goToKey(address);
async.doWhilst(function(done) {
  cursor.getCurrentBinary(function(key, value) {
    matches = (address === key);
    if (matches) {
      var txid = value.slice(4, 36);
      var outputIndex = value.readUInt32BE(36);
      var satoshis = value.readDoubleBE(40);
      var spentKey = txid.toString('binary') + outputIndex.toString('binary');
      outputs.push([spentKey, satoshis]);
    }
    setImmediate(done);
  });
}, function() {
  if (matches) {
    cursor.goToNext();
  }
  return matches;
}, function() {
  var balance = 0;
  for (var i = 0; i < outputs.length; i++) {
    var spent = txn.getBinary(spentDbi, outputs[i][0]);
    if (!spent) {
      balance += outputs[i][1];
    }
  }
  var end = new Date();
  console.log('Calculated balance in ' + (end - start) + ' milliseconds.');

  testDbi.close();
  env.close();

});
