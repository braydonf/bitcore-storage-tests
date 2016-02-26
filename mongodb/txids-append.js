'use strict';

var crypto = require('crypto');
var async = require('async');
var MongoClient = require('mongodb').MongoClient;

var TRANSACTIONS_PER_ADDRESS = 20;
var TOTAL_ADDRESSES = 2000;
var TOTAL_TRANSACTIONS = TOTAL_ADDRESSES * TRANSACTIONS_PER_ADDRESS;

var addressKeys = [];

console.log('Generating random test data...');

var c = 0;
while(c < TOTAL_ADDRESSES) {
  addressKeys.push(crypto.randomBytes(21));
  c++;
}

var heightAndTxids = [];

var d = 0;
while(d < TOTAL_TRANSACTIONS) {
  heightAndTxids.push(crypto.randomBytes(36));
  d++;
}

var url = 'mongodb://localhost:27017/txids-append';

MongoClient.connect(url, function(err, db) {
  if (err) {
    throw err;
  }
  var addressTxids = db.collection('addressTxids');

  console.log('Writing to database...');

  var start = new Date();

  var e = 0;

  async.timesSeries(TRANSACTIONS_PER_ADDRESS, function(n, done) {
    async.each(addressKeys, function(addressKey, next) {
      addressTxids.update({
        _id: addressKey
      }, {
        $push: {
          txids: heightAndTxids[e]
        }
      }, {
        upsert: true
      }, function(err) {
        if (err) {
          return next(err);
        }
        e++;
        next();
      });
    }, function(err) {
      if (err) {
        return done(err);
      }
      done();
    });
  }, function(err) {
    if (err) {
      throw err;
    }
    var end = new Date();

    console.log('Done writing ' + TOTAL_ADDRESSES + ' address indexes with ' +
                TRANSACTIONS_PER_ADDRESS + ' txids per address in ' + (end - start) + ' milliseconds.');

    db.close();

  });

});
