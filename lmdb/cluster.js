'use strict';

var path = require('path');
var cluster = require('cluster');
var crypto = require('crypto');
var http = require('http');
var numCPUs = require('os').cpus().length;

var bitcore = require('bitcore-lib');
var express = require('express');
var bodyParser = require('body-parser');
var async = require('async');
var lmdb = require('node-lmdb/build/Release/node-lmdb');

var constants = require('./constants');
var workers = [];

if (cluster.isMaster) {

  var env = new lmdb.Env();
  env.open({
    path: path.resolve(__dirname, './addresses'),
    maxDbs: 10,
    mapSize: 268435456 * 4096,
    maxReaders: 126
  });

  var addressDbi = env.openDbi({
    name: 'addressTxids',
    create: true,
    dupSort: true,
    dupFixed: true
  });

  var addresses = [];
  for (var i = 0; i < 2100; i++) {
    addresses.push(new bitcore.PrivateKey().toAddress().toString());
  }

  console.log(JSON.stringify(addresses));

  var txn = env.beginTxn();

  function randomHeightBuffer() {
    var heightBuffer = new Buffer(new Array(4));
    heightBuffer.writeUInt32BE(Math.round(Math.random() * 4000));
    return heightBuffer;
  }

  for (var j = 0; j < addresses.length; j++) {
    // Average of two transactions per address
    txn.putBinary(addressDbi, addresses[j], Buffer.concat([crypto.randomBytes(32), randomHeightBuffer()]));
    txn.putBinary(addressDbi, addresses[j], Buffer.concat([crypto.randomBytes(32), randomHeightBuffer()]));
  }
  txn.commit();
  addressDbi.close();
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
    path: path.resolve(__dirname, './addresses'),
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
  var addressDbi = env.openDbi({
    name: 'addressTxids'
  });

  var app = express();
  app.use(bodyParser.json());

  var txn = env.beginTxn({readOnly: true});

  app.post('/', function (req, res) {
    if (req.body.addresses) {
      var addresses = req.body.addresses;
      var cursor = new lmdb.Cursor(txn, addressDbi);
      var txidsHeight = {};
      var txids = [];
      async.eachSeries(addresses, function(address, next) {
        var matches = true;
        cursor.goToKey(address);
        async.doWhilst(function(done) {
          cursor.getCurrentBinary(function(key, value) {
            matches = (address === key);
            if (matches) {
              var txid = value.slice(0, 32).toString('hex');
              var height = value.readUInt32BE(32);
              if (!txidsHeight[txid]) {
                txidsHeight[txid] = height;
                txids.push(txid);
              }
            }
            done();
          });
        }, function() {
          if (matches) {
            cursor.goToNext();
          }
          return matches;
        }, next);
      }, function() {

        txids.sort(function(a, b) {
          return txidsHeight[a] - txidsHeight[b];
        });

        res.send('Success' + JSON.stringify(txids));
      });
    } else {
      res.send('None');
    }
  });

  var server = http.createServer(app).listen(9090);

  console.log('worker ' + process.pid + ' open');

  server.on('close', function() {
    console.log('Close!');
    txn.abort();
  });
}
