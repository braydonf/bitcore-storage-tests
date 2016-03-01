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


var txids = [];

var d = 0;
while(d < TOTAL_TRANSACTIONS) {
  txids.push({
    txid: crypto.randomBytes(32),
    height: Math.round(Math.random() * 1000)
  });
  d++;
}

var url = 'mongodb://localhost:27017/multi-txids';

MongoClient.connect(url, function(err, db) {
  if (err) {
    throw err;
  }
  console.log('Writing to database...');

  var startWrite = new Date();

  var txidsDb = db.collection('txids');
  txidsDb.createIndex({address: 1, height: -1});

  async.forEachOf(txids, function(data, n, next) {
    var addressKey = addressKeys[n % TOTAL_ADDRESSES];
    txidsDb.insertOne({
      address: addressKey,
      txid: data.txid,
      height: data.height
    }, next);
  }, function(err) {
    if (err) {
      throw err;
    }
    var endWrite = new Date();

    console.log('Finished writing to database in ' + (endWrite - startWrite) + ' milliseconds.');

    var start = new Date();

    txidsDb.find({
      address: {
        $in: addressKeys
      }
    }).toArray(function(err, docs) {
      if (err) {
        throw err;
      }
      assert.equal(docs.length, TOTAL_TRANSACTIONS);
      var end = new Date();
      console.log('Done reading txids for ' + TOTAL_ADDRESSES + ' in ' + (end - start) + ' milliseconds.');
      db.close();
    });
  });
});
