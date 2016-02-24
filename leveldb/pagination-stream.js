'use strict';

var path = require('path');
var crypto = require('crypto');
var levelup = require('levelup');

var db = levelup('./pages-stream');

var txids = [];

var SPACER = new Buffer('00', 'hex');
var TOTAL_TRANSACTIONS = 1000000;
var PAGE = 1000;
var PAGE_SIZE = 10;
var VALUE_SIZE = 36;
var HEIGHT_SIZE = 4;
var ADDRESS_KEY_SIZE = 21;
var SPACER_SIZE = 1;

var addressKey = crypto.randomBytes(21);

console.log('Generating random test data...');
var c = 0;
var operations = [];
while (c < TOTAL_TRANSACTIONS) {
  var height = crypto.randomBytes(4);
  var txid = crypto.randomBytes(32);
  operations.push({
    type: 'put',
    key: Buffer.concat([addressKey, SPACER, height, txid]),
    value: true
  });
  c++;
}

console.log('Starting to write test data...');
var a = new Date();
db.batch(operations, function(err) {
  if (err) {
    throw err;
  }
  var b = new Date();

  var writeTime = (b - a);

  console.log('Write value with ' + TOTAL_TRANSACTIONS + ' in ' + writeTime + ' milliseconds');
  console.log('Starting to read test data...');

  var c = new Date();

  var stream = db.createKeyStream({
    gt: Buffer.concat([addressKey, new Buffer('00', 'hex')]),
    lt: Buffer.concat([addressKey, new Buffer('ff', 'hex')]),
    keyEncoding: 'binary'
  });

  var count = 0;
  var start = (PAGE_SIZE * PAGE);
  var end = start + PAGE_SIZE;
  var txids = [];
  stream.on('data', function(data) {
    if (count >= start && count < end) {
      var offset = ADDRESS_KEY_SIZE + SPACER_SIZE;
      var height = data.readUInt32BE(offset);
      var txid = data.slice(offset + HEIGHT_SIZE, offset + VALUE_SIZE);
      txids.push({
        height: height,
        txid: txid
      });
    } else if (count > end) {
      stream.push(null);
    }
    count++;
  });

  stream.on('end', function() {
    var d = new Date();
    var readTime = (d - c);
    console.log('Read page ' + PAGE + ' in ' + readTime + ' milliseconds');
    console.log(txids);
    console.log(count);
  });

});

