'use strict';

var path = require('path');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;

var bitcore = require('bitcore-lib');
var async = require('async');
var lmdb = require('node-lmdb/build/Release/node-lmdb');

var encoding = require('./encoding');

function Chain(options) {
  EventEmitter.call(this);
  this.node = options.node;
  this.dbReady = false;
}

inherits(Chain, EventEmitter);

Chain.dependencies = ['bitcoind', 'db', 'web'];

Chain.prototype.start = function(callback) {
  var self = this;
  self.env = new lmdb.Env();
  self.env.open({
    path: path.resolve(__dirname, './blockdata'),
    maxDbs: 10,
    mapSize: 16 * 1024 * 1024 * 1024,
    noMetaSync: true,
    noSync: true
  });
  self.addressTxidsDbi = self.env.openDbi({
    name: 'addressTxids',
    create: true
  });
  self.addressBalanceDbi = self.env.openDbi({
    name: 'addressBalance',
    create: true
  });
  self.dbReady = true;
  setImmediate(callback);
};

Chain.prototype.stop = function(callback) {
  var self = this;
  if (this.dbReady) {
    self.addressTxidsDbi.close();
    self.addressBalanceDbi.close();
    self.env.close();
  }
  setImmediate(callback);
};

Chain.prototype.blockHandler = function(block, add, nextBlock) {
  var self = this;
  var action = add ? 'put' : 'del';

  function wait() {
    if (self.dbReady) {
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
    var txn = self.env.beginTxn();
    async.each(transactions, function processTransaction(tx, nextTransaction) {
      var txHash = tx.hash;
      var txHashBuffer = new Buffer(txHash, 'hex');
      async.parallel([
        function processInsAndOuts(inputsDone) {
          if (tx.isCoinbase()) {
            return inputsDone();
          }
          async.each(tx.inputs, function processIns(input, nextInput) {
            var prevTxId = input.prevTxId.toString('hex');
            self.node.services.bitcoind.getTransaction(prevTxId, false, function getTransaction(err, prevTxBuffer) {
              if (err) {
                return nextInput(err);
              }
              var prevTx = bitcore.Transaction().fromBuffer(prevTxBuffer);
              var prevOutput = prevTx.outputs[input.outputIndex];
              var script = prevOutput.script;
              if (!script) {
                return nextInput();
              }
              var addressInfo = encoding.extractAddressInfoFromScript(script, self.node.network);
              if (!addressInfo) {
                return nextInput();
              }
              var key = addressInfo.hashBuffer + addressInfo.hashTypeBuffer;
              var addressBalance = txn.getNumber(self.addressBalanceDbi, key) || 0;
              txn.putNumber(self.addressBalanceDbi, key, addressBalance - prevOutput.satoshis);
              var addressTxids = txn.getBinary(self.addressTxidsDbi, key) || new Buffer(Array(0));
              txn.putBinary(self.addressTxidsDbi, key, Buffer.concat([addressTxids, txHashBuffer]));
              nextInput();
            });
          }, inputsDone);
        },
        function(outputsDone) {
          async.each(tx.outputs, function processOuts(output, nextOutput) {
            var outputScript = output.script;
            if (!outputScript) {
              return nextOutput();
            }
            var outputAddressInfo = encoding.extractAddressInfoFromScript(outputScript, self.node.network);
            if (!outputAddressInfo) {
              return nextOutput();
            }
            var key = outputAddressInfo.hashBuffer + outputAddressInfo.hashTypeBuffer;
            var outputAddressBalance = txn.getNumber(self.addressBalanceDbi, key) || 0;
            txn.putNumber(self.addressBalanceDbi, key, outputAddressBalance + output.satoshis);
            var outputAddressTxids = txn.getBinary(self.addressTxidsDbi, key) || new Buffer(Array(0));;
            txn.putBinary(self.addressTxidsDbi, key, Buffer.concat([outputAddressTxids, txHashBuffer]));
            nextOutput();
          }, outputsDone);
        }
      ], nextTransaction);
    }, function(err) {
      if (err) {
        return nextBlock(err);
      }
      txn.commit();
      self.env.sync(function() {
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
