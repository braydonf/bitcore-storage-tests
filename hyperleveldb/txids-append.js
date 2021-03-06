'use strict';

var crypto = require('crypto');
var leveldown = require('leveldown-hyper');
var async = require('async');

var TRANSACTIONS_PER_ADDRESS = 2;
var TOTAL_ADDRESSES = 10000;
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

var e = 0;

var start = new Date();

var db = leveldown('./txids-append');
var operations = [];

db.open({
  createIfMissing: true
}, function() {

  console.log('Writing to database...');

  async.timesSeries(TRANSACTIONS_PER_ADDRESS, function(n, done) {
    async.each(addressKeys, function(addressKey, next) {
      db.get(addressKey, {
        encoding: 'binary'
      }, function(err, txids) {
        if (err && ((/notfound/i).test(err) || err.notFound)) {
          txids = new Buffer(new Array(0));
        } else if (err) {
          return next(err);
        }
        txids = Buffer.concat([txids, heightAndTxids[e]]);
        operations.push({
          type: 'put',
          key: addressKey,
          value: txids
        });
        e++;
        next();
      });
    }, function(err) {
      if (err) {
        return done(err);
      }
      db.batch(operations, function(err) {
        if (err) {
          return done(err);
        }
        done();
      });
    });
  }, function(err) {
    if (err) {
      throw err;
    }
    var end = new Date();

    console.log('Done writing ' + TOTAL_ADDRESSES + ' address indexes with ' +
                TRANSACTIONS_PER_ADDRESS + ' txids per address in ' + (end - start) + ' milliseconds.');

    db.close(function(err) {
      if (err) {
        throw err;
      }
    });

  });

});
