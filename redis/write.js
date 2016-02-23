'use strict';

var path = require('path');
var crypto = require('crypto');
var redis = require('redis');

var client = redis.createClient('/var/run/redis/redis.sock');

var addressOutputs = {};
var outputSpents = {};
var outKeys = [];

var TOTAL_TRANSACTIONS = 1000000;
var TOTAL_INS_AND_OUTS = TOTAL_TRANSACTIONS * 2; // Two inputs and outputs per transaction
var TOTAL_ADDRESSES = TOTAL_TRANSACTIONS / 2; // Two transactions per address

console.log('Generating random test data...');

var c = 0;
while (c < TOTAL_ADDRESSES) {
  outKeys.push(crypto.randomBytes(21).toString('binary'));
  c++;
}

var d = 0;
while (d < TOTAL_INS_AND_OUTS) {

  var key = crypto.randomBytes(36).toString('binary');
  var value = crypto.randomBytes(25);

  outputSpents[key] = value;

  var addressKey = outKeys[d % TOTAL_ADDRESSES];
  var addressKeyPlus = crypto.randomBytes(36);
  var addressValue = crypto.randomBytes(12);

  if (addressOutputs[addressKey]) {
    addressOutputs[addressKey].push([addressKeyPlus.toString('binary'), addressValue]);
  } else {
    addressOutputs[addressKey] = [[addressKeyPlus.toString('binary'), addressValue]];
  }

  d++;
}

console.log('Starting to write to database...');

var multi = client.multi();

for(var addressKey in addressOutputs) {
  var out = addressOutputs[addressKey];
  for (var i = 0; i < out.length; i++) {
    multi.set(addressKey + out[i][0], out[i][1]);
  }
}

for(var outKey in outputSpents) {
  multi.set(outKey, outputSpents[outKey]);
}

var start = new Date();

multi.exec(function (err, results) {
  if (err) {
    throw err;
  }
  var end = new Date();
  var time = (end - start) / 1000;
  console.log('Done writing ' + TOTAL_TRANSACTIONS + ' transactions in ' + time + ' seconds.');
});
