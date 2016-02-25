'use strict';

// Test stream from the highest number to the lowest, by storing records substracted
// from the maximum safe integer.

var path = require('path');
var crypto = require('crypto');
var levelup = require('levelup');

var db = levelup('./sorting');

var MIN_INTEGER_BUFFER = new Buffer(new Array(8));
MIN_INTEGER_BUFFER.writeDoubleBE(0);
var MAX_SAFE_INTEGER = Math.pow(2, 53) - 1;
var MAX_SAFE_INTEGER_BUFFER = new Buffer(new Array(8));
MAX_SAFE_INTEGER_BUFFER.writeDoubleBE(MAX_SAFE_INTEGER);

var RECORDS = 1000000;

var keys = [];
var operations = [];
for (var i = 0; i < RECORDS; i++) {
  var keyBuffer = new Buffer(new Array(8));
  keyBuffer.writeDoubleBE(MAX_SAFE_INTEGER - i);
  operations.push({
    type: 'put',
    key: keyBuffer
  });
}
db.batch(operations, function(err) {
  if (err) {
    throw err;
  }

  var start = new Date();

  var stream = db.createKeyStream({
    gt: MIN_INTEGER_BUFFER,
    lt: MAX_SAFE_INTEGER_BUFFER,
    keyEncoding: 'binary'
  });

  stream.on('data', function(data) {
    var key =  MAX_SAFE_INTEGER - data.readDoubleBE();
  });

  stream.on('end', function() {
    var delta = new Date() - start;
    console.log('Finished streaming ' + RECORDS + ' in ' + delta + ' milliseconds');
  });

});
