'use strict';

var path = require('path');
var cluster = require('cluster');
var http = require('http');
var numCPUs = require('os').cpus().length;

var bitcore = require('bitcore-lib');
var express = require('express');
var async = require('async');
var lmdb = require('node-lmdb/build/Release/node-lmdb');

var constants = require('./constants');
var workers = [];

if (cluster.isMaster) {

  var env = new lmdb.Env();
  env.open({
    path: path.resolve(__dirname, './data2'),
    maxDbs: 10,
    mapSize: 268435456 * 4096,
    maxReaders: 126
  });

  var testDbi = env.openDbi({
    name: 'test',
    create: true
  });

  var txn = env.beginTxn();
  var value = new Buffer(new Array(8));
  value.writeDoubleBE(32);
  var txid = new Buffer('8be7a86cc980a0e03845b84ce10b00fbd62534c11043d88adf3c2b7959518f64', 'hex');
  var txids = [];
  for (var i = 0; i < 1000; i++) {
    txids.push(txid);
  }
  var txidsBuffer = Buffer.concat(txids);
  txn.putBinary(testDbi, 'address1', Buffer.concat([value, txidsBuffer]));
  txn.commit();
  testDbi.close();
  env.close();

  console.log('Starting ' + numCPUs + ' workers...');
  for (var i = 0; i < numCPUs; i++) {
    var worker = cluster.fork();
    workers.push(worker);
  }

  // env.openDbi uses a write txn and only one can be
  // open in an environment at at time, thus we need
  // to start each worker individually
  function loadWorker(worker, next) {
    worker.send({start: true});
    setTimeout(function() {
      next();
    }, 2000);
  }

  async.eachSeries(workers, loadWorker, function() {});

  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });

} else {

  var env = new lmdb.Env();
  env.open({
    path: path.resolve(__dirname, './data2'),
    maxDbs: 10,
    mapSize: 268435456 * 4096,
    maxReaders: 126
  });

  process.on('message', function(msg) {
    if (msg.start) {
      start();
    }
  });

}

function start() {
  // This will need to be opened before syncing begins
  // as it requires exclusive write transaction
  var testDbi = env.openDbi({
    name: 'test'
  });

  var app = express();

  var txn = env.beginTxn({readOnly: true});

  app.get('/', function (req, res) {
    if (req.query.key) {
      var key = req.query.key;
      var value = txn.getBinary(testDbi, key);
      if (value) {
        var num = value.readDoubleBE();
        var txids = [];
        var pos = 8;
        while (pos < 328 || pos < value.length) {
          var txid = value.slice(pos, pos + 32);
          txids.push(txid.toString('hex'));
          pos = pos + 32;
        }

        var result = {
          balance: num,
          txids: txids
        };

        var resultStr = JSON.stringify(result);

        res.send('Value for key (' + key + '): ' + resultStr);
      } else {
        res.send('Not found');
      }
    } else {
      res.send('No value');
    }

  });

  var server = http.createServer(app).listen(9090);

  console.log('worker ' + process.pid + ' open');

  server.on('close', function() {
    console.log('Close!');
    txn.abort();
  });
}
