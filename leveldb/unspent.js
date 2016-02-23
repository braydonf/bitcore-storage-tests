'use strict';

var path = require('path');
var crypto = require('crypto');
var levelup = require('levelup');

var db = levelup('./test-write');

var addressKeys = [];
var unspentOutputs = [];

var SPACER = new Buffer('00', 'hex');
var UNSPENT_PREFIX = new Buffer('01', 'hex');

var TOTAL_ADDRESSES = 2100;
var TOTAL_OUTPUTS = TOTAL_ADDRESSES * 1; // One output per address

console.log('Generating random test data...');

var c = 0;
while (c < TOTAL_ADDRESSES) {
  addressKeys.push(crypto.randomBytes(21));
  c++;
}

var d = 0;
while (d < TOTAL_OUTPUTS) {

  var heightTxIdOutputIndex = crypto.randomBytes(40);
  var addressKey = addressKeys[d % TOTAL_ADDRESSES];
  var fullKey = Buffer.concat([addressKey, SPACER, heightTxIdOutputIndex]);

  unspentOutputs[fullKey.toString('binary')] = true;

  d++;
}

console.log('Creating batch operations...');

var operations = [];

for(var addressKey in unspentOutputs) {
  operations.push({
    type: 'put',
    key: new Buffer(addressKey, 'binary'),
    value: true
  });
}

console.log('Starting to write ' + operations.length + ' operations...');

var start = new Date();

db.batch(operations, function(err) {
  if (err) {
    throw err;
  }
  var end = new Date();
  var time = (end - start) / 1000;
  console.log('Done writing ' + TOTAL_OUTPUTS + ' outputs in ' + time + ' seconds.');

  var streamStart = new Date();
  var streamsCount = addressKeys.length;
  var streamsEnded = 0;
  var streams = [];
  var outputs = [];
  for(var i = 0; i < addressKeys.length; i++) {
    var addressKey = addressKeys[i];
    var stream = db.createKeyStream({
      gt: Buffer.concat([addressKey, new Buffer('00', 'hex')]),
      lt: Buffer.concat([addressKey, new Buffer('ff', 'hex')]),
      keyEncoding: 'binary'
    });
    stream.on('data', function(key) {
      var height = key.readUInt32BE(22);
      var txid = key.slice(26, 58);
      var outputIndex = key.readUInt32BE(58);
      outputs.push([height, txid, outputIndex]);
    });
    stream.on('end', function() {
      streamsEnded++;
      if (streamsCount === streamsEnded) {
        outputs.sort(function(a, b) {
          return a[0] - b[0];
        });
        var delta = (new Date() - streamStart);
        console.log('Finished getting all sorted unspent output positions in ' + delta +
                    ' milliseconds for ' + TOTAL_ADDRESSES + ' addresses.');
      }

    });
    streams.push(stream);
  }

});
