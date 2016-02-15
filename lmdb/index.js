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
  this.dbOpen = false;
  this.blocks = [];
  this.lastCommit = new Date();
}

inherits(Chain, EventEmitter);

Chain.COMMIT_INTERVAL = 60 * 1000; // 1 minute

Chain.dependencies = ['bitcoind', 'db', 'web'];

Chain.prototype.start = function(callback) {
  var self = this;
  self.env = new lmdb.Env();
  self.env.open({
    path: path.resolve(__dirname, './data'),
    maxDbs: 10,
    mapSize: 268435456 * 4096,
    noMetaSync: true,
    noSync: true
  });
  self.unspentOutputsDbi = self.env.openDbi({
    name: 'unspentOutputs',
    create: true
  });
  self.addressSumDbi = self.env.openDbi({
    name: 'addressSum',
    create: true,
    dupSort: true,
    dupFixed: true
  });
  self.dbOpen = true;
  setImmediate(callback);
};

Chain.prototype.stop = function(callback) {
  var self = this;

  if (self.dbOpen) {
    self.unspentOutputsDbi.close();
    self.addressSumDbi.close();
    self.env.close();
    setImmediate(callback);
  } else {
    setImmediate(callback);
  }

};

Chain.prototype.blockHandler = function(block, add, nextBlock) {
  var self = this;
  var action = add ? 'put' : 'del';

  if (!self.txn) {
    self.txn = self.env.beginTxn();
  }

  function wait() {
    if (self.dbOpen) {
      addBlock();
    } else {
      setTimeout(wait, 1000);
    }
  }

  wait();

  function addBlock() {

    var blockHash = block.hash;
    var prevBlockHash = block.prevHash;
    var transactions = block.transactions;
    var transactionLength = transactions.length;

    for (var t = 0; t < transactions.length; t++) {
      var tx = transactions[t];

      var txHash = tx.hash;
      var txHashBuffer = new Buffer(txHash, 'hex');

      if (!tx.isCoinbase()) {
        for (var inputIndex = 0; inputIndex < tx.inputs.length; inputIndex++) {
          var input = tx.inputs[inputIndex];

          var prevTxId = input.prevTxId.toString('hex');
          var prevOutputKey = prevTxId + '-' + input.outputIndex;
          var prevOutputValue = self.txn.getBinary(self.unspentOutputsDbi, prevOutputKey);

          if (prevOutputValue) {
            var prevOutputSatoshis = prevOutputValue.readDoubleBE();
            var hashBuffer = prevOutputValue.slice(8, 28);
            var hashTypeBuffer = prevOutputValue.slice(28, 29);

            var key = hashBuffer + hashTypeBuffer;
            var addressSum = self.txn.getBinary(self.addressSumDbi, key);
            var balance = addressSum.readDoubleBE() - prevOutputSatoshis;
            addressSum.writeDoubleBE(balance);
            var value = Buffer.concat([addressSum, txHashBuffer]);
            self.txn.putBinary(self.addressSumDbi, key, value);
          }
        }
      }

      for (var outputIndex = 0; outputIndex < tx.outputs.length; outputIndex++) {
        var output = tx.outputs[outputIndex];

        var outputScript = output.script;
        if (!outputScript) {
          continue;
        }
        var outputAddressInfo = encoding.extractAddressInfoFromScript(outputScript, self.node.network);
        if (!outputAddressInfo) {
          continue;
        }

        var outputKey = txHash + '-' + outputIndex;
        var outputSatoshis = new Buffer(new Array(8));
        outputSatoshis.writeDoubleBE(output.satoshis);
        var outputValue = Buffer.concat([outputSatoshis, outputAddressInfo.hashBuffer, outputAddressInfo.hashTypeBuffer]);
        self.txn.putBinary(self.unspentOutputsDbi, outputKey, outputValue);

        var outKey = outputAddressInfo.hashBuffer + outputAddressInfo.hashTypeBuffer;
        var outputAddressSum = self.txn.getBinary(self.addressSumDbi, outKey) || new Buffer(new Array(8));
        var outputBalance = output.satoshis;
        outputBalance += outputAddressSum.readDoubleBE();
        outputAddressSum.writeDoubleBE(outputBalance);
        var outValue = Buffer.concat([outputAddressSum, txHashBuffer]);
        self.txn.putBinary(self.addressSumDbi, outKey, outValue);
      }
    }

    if (new Date() - self.lastCommit > self.COMMIT_INTERVAL || self.blocks.length > 200) {
      // TODO: commit tip at the same time
      self.txn.commit();
      self.env.sync(function(err){
        self.lastCommit = new Date();
        if (err) {
          return nextBlock(err);
        }
        self.txn = false;
        nextBlock(null, []);
      });
    } else {
      setImmediate(function() {
        nextBlock(null, []);
      });
    }
  }

};

Chain.prototype.getAPIMethods = function() {
  return [];
};

Chain.prototype.getPublishEvents = function() {
  return [];
};

module.exports = Chain;
