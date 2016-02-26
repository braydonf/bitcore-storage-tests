'use strict';

var crypto = require('crypto');
var async = require('async');
var MongoClient = require('mongodb').MongoClient;

var TOTAL_ADDRESSES = 10000;

var addressKeys = [];
var balances = [];
var received = [];
var change = [];
var count = [];

console.log('Generating random test data...');

var c = 0;
while(c < TOTAL_ADDRESSES) {
  addressKeys.push(crypto.randomBytes(21));
  balances.push(Math.round(Math.random() * 1000));
  received.push(Math.round(Math.random() * 1000));
  change.push(Math.round(Math.random() * 1000));
  count.push(Math.round(Math.random() * 1000));
  c++;
}

var url = 'mongodb://localhost:27017/multi-summary';

MongoClient.connect(url, function(err, db) {
  if (err) {
    throw err;
  }
  console.log('Writing to database...');
  var start = new Date();

  var summaryDb = db.collection('summary');

  async.forEachOf(addressKeys, function(addressKey, n, next) {
    summaryDb.insertOne({
      _id: addressKey,
      balance: balances[n],
      received: received[n],
      change: change[n],
      count: count[n]
    }, next);
  }, function(err) {
    if (err) {
      throw err;
    }
    console.log('Finished writing to database...');

    summaryDb.find({
      _id: {
        $in: addressKeys
      }
    }, function(err, results) {
      if (err) {
        throw err;
      }
      var data = results.toArray();
      data.then(function(summaries) {
        var balance = 0;
        for(var i = 0; i < summaries.length; i++) {
          balance += summaries[i].balance;
        }
        console.log('Balance', balance);
        var end = new Date();
        console.log('Done reading balance for ' + TOTAL_ADDRESSES + ' in ' + (end - start) + ' milliseconds.');
        db.close();
      });

    });

  });
});
