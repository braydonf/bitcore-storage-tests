'use strict';

var bitcore = require('bitcore-lib');
var async = require('async');
var MongoClient = require('mongodb').MongoClient;
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;

var encoding = require('./encoding');

function Chain(options) {
  EventEmitter.call(this);
  this.cache = {};
  this.node = options.node;
  this.db = null;
  this.blocks = [];
}

inherits(Chain, EventEmitter);

Chain.dependencies = ['bitcoind', 'db', 'web'];

Chain.prototype.start = function(callback) {
  var self = this;
  var url = 'mongodb:///tmp/mongodb-27017.sock/bitcoreMongo';

  MongoClient.connect(url, function(err, db) {
    if (err) {
      return callback(err);
    }
    self.db = db;
    self.prevOutsDb = self.db.collection('prevOuts');
    self.addressSummaryDb = self.db.collection('addressSummary');
    self.addressSummaryDb.createIndex({_id: 1, height: -1});
    self.addressTxidsDb = self.db.collection('addressTxids');
    self.addressTxidsDb.createIndex({address: 1, height: -1});
    callback();
  });
};

Chain.prototype.stop = function(callback) {
  if (this.db) {
    this.db.close();
  }
  setImmediate(callback);
};

Chain.prototype.blockHandler = function(block, add, nextBlock) {
  var self = this;
  var action = add ? 'put' : 'del';

  function wait() {
    if (self.db) {
      processBlock();
    } else {
      setTimeout(wait, 1000);
    }
  }

  wait();

  function processBlock() {
    var blockHash = block.hash;
    var height = block.__height;
    var heightBuffer = new Buffer(new Array(4));
    heightBuffer.writeUInt32BE(height);
    var prevBlockHash = block.prevHash;
    var transactions = block.transactions;
    var transactionLength = transactions.length;

    var summaryDeltas = {};
    var summaryTxids = {};

    async.eachSeries(transactions, function processTransaction(transaction, nextTransaction) {
      var txHash = transaction.hash;
      var txHashBuffer = new Buffer(txHash, 'hex');

      var addressesInTransaction = {};

      async.parallel([
        function processInputs(inputsDone) {
          if (transaction.isCoinbase()) {
            return inputsDone();
          }
          async.each(transaction.inputs, function processInput(input, nextInput) {
            var prevOutId = input.prevTxId.toString('hex') + input.outputIndex;
            self.prevOutsDb.findOne({_id: prevOutId}, function findPreviousOutput(err, prevOut){
              if (err) {
                return nextInput(err);
              }
              if (prevOut) {
                var key = prevOut.address.buffer.toString('hex');
                var balanceDiff = add ? prevOut.satoshis * -1 : prevOut.satoshis;

                if (summaryDeltas[key]) {
                  summaryDeltas[key].balance += balanceDiff;
                } else {
                  summaryDeltas[key] = {
                    balance: balanceDiff
                  };
                }
                addressesInTransaction[key] = true;
              }
              nextInput();
            });
          }, inputsDone);
        },
        function(outputsDone) {
          async.forEachOf(transaction.outputs, function processOutput(output, outputIndex, nextOutput) {
            var script = output.script;
            if (!script) {
              return nextOutput();
            }
            var addressInfo = encoding.extractAddressInfoFromScript(script, self.node.network);
            if (!addressInfo) {
              return nextOutput();
            }

            var key = Buffer.concat([addressInfo.hashBuffer, addressInfo.hashTypeBuffer]).toString('hex');
            var balanceDiff = add ? output.satoshis * -1 : output.satoshis;

            if (summaryDeltas[key]) {
              summaryDeltas[key].balance += balanceDiff;
            } else {
              summaryDeltas[key] = {
                balance: balanceDiff
              };
            }

            addressesInTransaction[key] = true;

            var prevOutId = txHash + outputIndex;

            self.prevOutsDb.insert({
              _id: prevOutId,
              address: Buffer.concat([addressInfo.hashBuffer, addressInfo.hashTypeBuffer]),
              satoshis: output.satoshis
            }, nextOutput);

          }, outputsDone);
        }
      ], function() {
        for (var key in addressesInTransaction) {
          if (summaryTxids[key]) {
            summaryTxids[key].push(txHashBuffer);
          } else {
            summaryTxids[key] = [txHashBuffer];
          }
        }
        nextTransaction();
      });
    }, function(err) {
      if (err) {
        return nextBlock(err);
      }

      async.parallel([
        function(txidsDone) {
          async.forEachOf(summaryTxids, function(txids, addressKey, nextTxids) {
            async.each(txids, function(txid, nextTxid) {
              self.addressTxidsDb.insert({
                address: addressKey,
                height: block.height,
                txid: txid
              }, nextTxid);
            }, nextTxids);
          }, txidsDone);
        },
        function(deltasDone) {
          async.forEachOf(summaryDeltas, function(delta, addressKey, nextAddress) {
            self.addressSummaryDb.update({
              _id: addressKey
            }, {
              $set: {
                height: height
              },
              $inc: {
                balance: delta.balance
              }
            }, {
              upsert: true
            }, nextAddress);
          }, deltasDone);
        }
      ], function(err) {
        if (err) {
          return nextBlock(err);
        }
        nextBlock(null, []);
      });

    });
  }
};

Chain.prototype.getAPIMethods = function() {
  return [];
};

Chain.prototype.getPublishEvents = function() {
  return [];
};

module.exports = Chain;
