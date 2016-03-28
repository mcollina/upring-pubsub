'use strict'

const UpRing = require('upring')
const inherits = require('util').inherits
const MQEmitter = require('mqemitter')

function UpRingPubSub (opts) {
  if (!(this instanceof UpRingPubSub)) {
    return new UpRingPubSub(opts)
  }

  this.upring = new UpRing(opts)

  MQEmitter.call(this, opts)
}

inherits(UpRingPubSub, MQEmitter)

UpRingPubSub.prototype.whoami = function () {
  return this.upring.whoami()
}

module.exports = UpRingPubSub
