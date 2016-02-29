'use strict';

var assert = require('assert');
var crypto = require('crypto');
var async = require('async');
var MongoClient = require('mongodb').MongoClient;

var TRANSACTIONS_PER_ADDRESS = 2;
var TOTAL_ADDRESSES = 100000;
var TOTAL_TRANSACTIONS = TOTAL_ADDRESSES * TRANSACTIONS_PER_ADDRESS;

var addressKeys = [];
console.log('Generating random test data...');

var c = 0;
while(c < TOTAL_ADDRESSES) {
  addressKeys.push(crypto.randomBytes(21));
  c++;
}

var operations = [];

var d = 0;
while(d < TOTAL_TRANSACTIONS / TRANSACTIONS_PER_ADDRESS) {
  var txCount = 0;
  var values = [];
  while(txCount < TRANSACTIONS_PER_ADDRESS) {
    values.push({
      txid: crypto.randomBytes(32),
      height: Math.round(Math.random(400000))
    });
    txCount++;
  }
  operations.push({
    key: addressKeys[d % TOTAL_ADDRESSES],
    value: values
  });
  d++;
}

var url = 'mongodb://localhost:27017/multi-txids';

MongoClient.connect(url, function(err, db) {
  if (err) {
    throw err;
  }
  console.log('Writing to database...');

  var txidsDb = db.collection('txids');
  txidsDb.createIndex({address: 1});

  async.forEachOf(operations, function(op, n, next) {
    txidsDb.insertOne({
      address: op.key,
      txids: op.value,
    }, next);
  }, function(err) {
    if (err) {
      throw err;
    }
    console.log('Finished writing to database...');

    var start = new Date();

    txidsDb.find({
      address: {
        $in: addressKeys
      }
    }).toArray(function(err, docs) {
      if (err) {
        throw err;
      }
      var txids = [];
      for (var i = 0; i < docs.length; i++) {
        for (var j = 0; j < docs[i].txids.length; j++) {
          txids.push(docs[i].txids[j]);
        }
      }
      txids.sort(function(a, b) {
        return a.height - b.height;
      });
      assert.equal(txids.length, TOTAL_TRANSACTIONS);
      var end = new Date();
      console.log('Done reading txids for ' + TOTAL_ADDRESSES + ' in ' + (end - start) + ' milliseconds.');
      db.close();
    });
  });
});
