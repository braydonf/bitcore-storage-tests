'use strict';

var assert = require('assert');
var crypto = require('crypto');
var async = require('async');
var levelup = require('levelup');

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
    values.push(crypto.randomBytes(36));
    txCount++;
  }
  operations.push({
    type: 'put',
    key: addressKeys[d % TOTAL_ADDRESSES],
    value: Buffer.concat(values)
  });
  d++;
}

var db = levelup('./multi-txids');

var startWrite = new Date();

db.batch(operations, function(err) {
  if (err) {
    return callback(err);
  }

  var endWrite = new Date();

  console.log('Finished writing to database in ' + (endWrite - startWrite) + ' milliseconds.');

  var start = new Date();

  var values = [];

  async.each(addressKeys, function(key, next) {
    db.get(key, {
      valueEncoding: 'binary'
    }, function(err, value) {
      if (err) {
        return next(err);
      }
      var pos = 0;
      while(pos < value.length) {
        var height = value.readUInt32BE(0);
        var txid = value.slice(4, 36);
        values.push([height, txid]);
        pos += 36;
      }
      next();
    });
  }, function(err) {
    if (err) {
      throw err;
    }

    values.sort(function(a, b) {
      return a[0] - b[0];
    });

    assert.equal(values.length, TOTAL_TRANSACTIONS);

    var end = new Date();
    console.log('Done reading txids for ' + TOTAL_ADDRESSES + ' in ' + (end - start) + ' milliseconds.');

  });

});
