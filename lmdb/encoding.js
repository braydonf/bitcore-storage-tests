'use strict';

var bitcore = require('bitcore-lib');
var Address = bitcore.Address;
var PublicKey = bitcore.PublicKey;
var constants = require('./constants');

var exports = {};

exports.extractAddressInfoFromScript = function(script, network) {
  var hashBuffer;
  var addressType;
  var hashTypeBuffer;
  if (script.isPublicKeyHashOut()) {
    hashBuffer = script.chunks[2].buf;
    hashTypeBuffer = constants.HASH_TYPES.PUBKEY;
    addressType = Address.PayToPublicKeyHash;
  } else if (script.isScriptHashOut()) {
    hashBuffer = script.chunks[1].buf;
    hashTypeBuffer = constants.HASH_TYPES.REDEEMSCRIPT;
    addressType = Address.PayToScriptHash;
  } else {
    return false;
  }
  return {
    hashBuffer: hashBuffer,
    hashTypeBuffer: hashTypeBuffer,
    addressType: addressType
  };
};

module.exports = exports;
