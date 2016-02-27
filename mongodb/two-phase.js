'use strict';

var async = require('async');
var bitcore = require('bitcore-lib');
var MongoClient = require('mongodb').MongoClient;
var url = 'mongodb://localhost:27017/two-phase';
var blockData = require('block.json');
var block = bitcore.Block.fromBuffer(new Buffer(blockData, 'hex'));

MongoClient.connect(url, function(err, db) {
  if (err) {
    throw err;
  }
  console.log('Writing to database...');
  var start = new Date();

  var blocksDb = db.collection('blocks');
  var summaryDb = db.collection('summary');
  var txidsDb = db.collection('txids');
  txidsDb.createIndex({address: 1, spent: 1, height: -1});

  function startCommit(block, done) {
    blocksDb.update({
      _id: block.hash
    }, {
      $set: {
        status: 'pending'
      }
    }, {
      upsert: true
    }, done);
  }

  function endCommit(block, done) {
    blocksDb.update({
      _id: block.hash
    }, {
      $set: {
        status: 'commited',
        count: block.transactions.length,
        bytes: block.toBuffer().length
      }
    }, done);
  }

  startCommit(function(err) {
    if (err) {
      throw err;
    }
    async.eachSeries(block.transactions, function(transaction, next) {

      // calculate balance difference for each address

      // mark txids as spent

      // add previous outputs if hasn't already been added

      // update address summaries if height is less than this block

    }, endCommit);
  });

});
