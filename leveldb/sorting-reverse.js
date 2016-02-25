'use strict';

// Test stream from the highest number to the lowest by streaming
// keys in reverse.

var path = require('path');
var crypto = require('crypto');
var levelup = require('levelup');

var db = levelup('./sorting-reverse');

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
  keyBuffer.writeDoubleBE(i);
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
    keyEncoding: 'binary',
    reverse: true
  });

  stream.on('data', function(data) {
    var key =  data.readDoubleBE();
  });

  stream.on('end', function() {
    var delta = new Date() - start;
    console.log('Finished streaming ' + RECORDS + ' in ' + delta + ' milliseconds');
  });

});
