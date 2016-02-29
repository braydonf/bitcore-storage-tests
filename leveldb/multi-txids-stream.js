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
while(d < TOTAL_TRANSACTIONS) {
  operations.push({
    type: 'put',
    key: Buffer.concat([addressKeys[d % TOTAL_ADDRESSES], new Buffer('00', 'hex'), crypto.randomBytes(36)]),
  });
  d++;
}

var db = levelup('./multi-txids-stream');

db.batch(operations, function(err) {
  if (err) {
    return callback(err);
  }

  var start = new Date();

  console.log('Starting to read values...');

  var values = [];

  async.each(addressKeys, function(key, next) {
    var stream = db.createKeyStream({
      gt: Buffer.concat([key, new Buffer('00', 'hex')]),
      lt: Buffer.concat([key, new Buffer('ff', 'hex')]),
      keyEncoding: 'binary',
      keyAsBuffer: true
    });
    stream.on('data', function(keyBuffer) {
      var height = keyBuffer.readUInt32BE(22, 26);
      var txid = keyBuffer.slice(26, 58);
      values.push([height, txid]);
    });
    var error;
    stream.on('error', function(err) {
      error = err;
    });
    stream.on('end', function() {
      if (error) {
        return next(error);
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
