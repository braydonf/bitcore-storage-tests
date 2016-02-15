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
  var url = 'mongodb://localhost:27017/bitmongo';

  MongoClient.connect(url, function(err, db) {
    if (err) {
      return callback(err);
    }
    self.db = db;
    self.transactions = self.db.collection('transactions');
    self.addressSummary = self.db.collection('addressSummary');
    self.addressSummary.createIndex({hash: 1, type: 1}, null, function(err) {
      if (err) {
        return callback(err);
      }
      callback();
    });

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
    var prevBlockHash = block.prevHash;
    var transactions = block.transactions;
    var transactionLength = transactions.length;
    
    async.eachSeries(transactions, function processTransaction(transaction, nextTransaction) {
      var txObject = transaction.toObject();
      var txHash = txObject.hash;
      txObject._id = txHash; // Use transaction hash as mongodb _id
      delete txObject.hash;

      self.transactions.save(txObject, function saveTransaction(err) {
        if (err) {
          return nextTransaction(err);
        }
        async.parallel([
          function processInputs(inputsDone) {
            if (transaction.isCoinbase()) {
              return inputsDone();
            }
            async.each(transaction.inputs, function processInput(input, nextInput) {
              var prevTxId = input.prevTxId.toString('hex');
              self.transactions.findOne({_id: prevTxId}, function findPreviousOutput(err, prevTx){
                if (err) {
                  return nextInput(err);
                } else if (!prevTx) {
                  return nextInput(new Error('Previous output (' + prevTxId + ') not found.'));
                }
                var output = prevTx.outputs[input.outputIndex];
                var script;
                try {
                  script = bitcore.Script(script);
                } catch(e) {
                  return nextInput();
                }
                var addressInfo = encoding.extractAddressInfoFromScript(script, self.node.network);
                if (!addressInfo) {
                  return nextInput();
                }
                
                // TODO when rewinding $pull txid and $inc + instead of - 

                self.addressSummary.update({
                  hash: addressInfo.hashBuffer,
                  type: addressInfo.hashTypeBuffer
                }, {
                  $inc: {
                    balance: output.satoshis * -1,
                  },
                  $push: {
                    txids: txHash
                  }
                }, {
                  upsert: true
                }, nextInput);
              });
            }, inputsDone);
          },
          function(outputsDone) {
            async.each(transaction.outputs, function processOutput(output, nextOutput) {
              var script = output.script;
              if (!script) {
                return nextOutput();
              }
              var addressInfo = encoding.extractAddressInfoFromScript(script, self.node.network);
              if (!addressInfo) {
                return nextOutput();
              }
              self.addressSummary.update({
                hash: addressInfo.hashBuffer,
                type: addressInfo.hashTypeBuffer
              }, {
                $inc: {
                  balance: output.satoshis
                },
                $push: {
                  txids: txHash
                }
              }, {
                upsert: true
              }, nextOutput);
            }, outputsDone);
          }
        ], nextTransaction);

      });
    }, function(err) {
      if (err) {
        return nextBlock(err);
      }
      nextBlock(null, []);
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
