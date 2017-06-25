'use strict'

const mqemitter = require('mqemitter')
const Receiver = require('./lib/receiver')
const commands = require('./lib/commands')
const extractBase = require('./lib/extractBase')
const untrack = Symbol('untrack')
const upwrap = Symbol('upwrap')

module.exports = function (upring, opts, next) {
  if (upring.pubsub) {
    return next(new Error('pubsub property already exist'))
  }
  upring.pubsub = new UpRingPubSub(upring, opts)
  next()
}

function UpRingPubSub (upring, opts) {
  if (!(this instanceof UpRingPubSub)) {
    return new UpRingPubSub(upring, opts)
  }

  opts = opts || {}

  this.upring = upring
  this._internal = mqemitter(opts)

  this._receivers = new Map()

  this.closed = false

  // expose the parent logger
  this.logger = this.upring.logger

  commands(this)

  this.upring.on('peerUp', (peer) => {
    // TODO maybe we should keep track of the wildcard
    // receivers in a list, and walk through there
    for (var receiver in this._receivers) {
      if (receiver.peers) {
        receiver.peers.push(peer)
        receiver.sendPeer(peer, 0, noop)
      }
    }
  })

  this.upring.on('close', this.close.bind(this, noop))
}

// do we have a wildcard?
function hasLowWildCard (topic) {
  const levels = topic.split('/')

  return levels[0] === '#' || levels[1] === '#' ||
         levels[0] === '+' || levels[1] === '+'
}

Object.defineProperty(UpRingPubSub.prototype, 'current', {
  get: function () {
    return this._internal.current
  }
})

UpRingPubSub.prototype.emit = function (msg, cb) {
  if (!this.upring.isReady) {
    this.upring.once('up', this.emit.bind(this, msg, cb))
    return
  }

  const key = extractBase(msg.topic)
  const logger = this.logger
  const data = {
    cmd: 'publish',
    ns: 'pubsub',
    key,
    msg
  }

  logger.debug(data, 'sending request')
  this.upring.request(data, cb || noop)
}

UpRingPubSub.prototype.on = function (topic, onMessage, done) {
  if (!this.upring.isReady) {
    this.upring.once('up', this.on.bind(this, topic, onMessage, done))
    return
  }

  done = done || noop

  const key = extractBase(topic)
  if (!onMessage[upwrap]) {
    onMessage[upwrap] = (msg, cb) => {
      onMessage.call(this, msg, cb)
    }
  }

  this._internal.on(topic, onMessage[upwrap])

  var peers = null

  // data is already flowing through this instance
  // nothing to do
  if (this._receivers.has(topic)) {
    this.logger.info({ topic }, 'subscription already setup')
    this._receivers.get(topic).count++
    process.nextTick(done)
    return
  } else if (hasLowWildCard(topic)) {
    peers = this.upring.peers(false)
  } else if (this.upring.allocatedToMe(key)) {
    this.logger.info({ topic }, 'local subscription')

    onMessage[untrack] = this.upring.track(key).on('move', () => {
      // resubscribe if it is moved to someone else
      onMessage[untrack] = undefined
      setImmediate(() => {
        this.removeListener(topic, onMessage, () => {
          this.on(topic, onMessage, () => {
            this.logger.info({ topic }, 'resubscribed because topic moved to another peer')
          })
        })
      })
    })

    process.nextTick(done)

    return this
  }

  // normal case, we just need to go to a single instance
  const receiver = new Receiver(this, topic, key, peers)
  this._receivers.set(topic, receiver)
  receiver.send(done)

  return this
}

UpRingPubSub.prototype.removeListener = function (topic, onMessage, done) {
  const receiver = this._receivers.get(topic)

  if (onMessage[untrack]) {
    onMessage[untrack].end()
    onMessage[untrack] = undefined
  }

  if (receiver && --receiver.count === 0) {
    receiver.unsubscribe()
  }

  this._internal.removeListener(topic, onMessage[upwrap], done)
}

UpRingPubSub.prototype.close = function (cb) {
  cb = cb || noop
  if (!this.upring.isReady) {
    this.upring.once('up', this.close.bind(this, cb))
    return
  }

  if (this.closed) {
    cb()
    return
  }

  this._receivers.forEach((value, key) => {
    value.unsubscribe()
  })

  this.closed = true

  this._internal.close(cb)
}

function noop () {}
