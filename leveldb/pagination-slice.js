'use strict';

var path = require('path');
var crypto = require('crypto');
var levelup = require('levelup');

var db = levelup('./pages');

var txids = [];

var TOTAL_TRANSACTIONS = 1000000;
var PAGE = 99999;
var PAGE_SIZE = 10;
var VALUE_SIZE = 36;
var HEIGHT_SIZE = 4;

var addressKey = crypto.randomBytes(21);

console.log('Generating random test data...');
var c = 0;
while (c < TOTAL_TRANSACTIONS) {
  var height = crypto.randomBytes(4);
  var txid = crypto.randomBytes(32);
  txids.push(Buffer.concat([height, txid]));
  c++;
}

console.log('Starting to write test data...');
var a = new Date();
db.put(addressKey, Buffer.concat(txids), function(err) {
  if (err) {
    throw err;
  }
  var b = new Date();

  var writeTime = (b - a);

  console.log('Write value with ' + TOTAL_TRANSACTIONS + ' in ' + writeTime + ' milliseconds');
  console.log('Starting to read test data...');
  var c = new Date();
  db.get(addressKey, {
    valueEncoding: 'binary'
  }, function(err, value) {

    var start = VALUE_SIZE * PAGE * PAGE_SIZE;
    var end = start + (PAGE_SIZE * VALUE_SIZE);

    var sliced = value.slice(start, end);

    var pos = 0;

    var txids = [];

    while(pos < sliced.length) {
      var height = sliced.readUInt32BE(pos);
      var txid = sliced.slice(pos + HEIGHT_SIZE, pos + VALUE_SIZE);
      txids.push({
        height: height,
        txid: txid
      });
      pos += VALUE_SIZE;
    }

    var d = new Date();

    var pageTime = (d - c);

    console.log('Read page ' + PAGE + ' in ' + pageTime + ' milliseconds');
    console.log(txids);

  });

});

