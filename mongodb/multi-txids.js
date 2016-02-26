'use strict';

var crypto = require('crypto');
var async = require('async');
var MongoClient = require('mongodb').MongoClient;

var TRANSACTIONS_PER_ADDRESS = 2;
var TOTAL_ADDRESSES = 2000;
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
    height: Math.round(Math.random() * 1000),
    spent: (Math.round(Math.random()) === 1)
  });
  d++;
}

var url = 'mongodb://localhost:27017/multi-txids';

MongoClient.connect(url, function(err, db) {
  if (err) {
    throw err;
  }
  console.log('Writing to database...');
  var start = new Date();

  var txidsDb = db.collection('txids');
  txidsDb.createIndex({address: 1, spent: 1, height: -1});

  var e = 0;

  async.forEachOf(addressKeys, function(addressKey, n, next) {
    var data = txids[e];
    txidsDb.insertOne({
      address: addressKey,
      txid: data.txid,
      height: data.height,
      spent: data.spent
    }, next);
  }, function(err) {
    if (err) {
      throw err;
    }
    console.log('Finished writing to database...');

    txidsDb.find({
      address: {
        $in: addressKeys
      },
      spent: false
    }, function(err, results) {
      if (err) {
        throw err;
      }
      var data = results.toArray();
      data.then(function(txids) {
        var end = new Date();
        console.log('Done reading txids for ' + TOTAL_ADDRESSES + ' in ' + (end - start) + ' milliseconds.');
        db.close();
      });
    });

  });
});
