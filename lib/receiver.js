'use strict'

const streams = require('readable-stream')
const eos = require('end-of-stream')
const steed = require('steed')
const inherits = require('util').inherits
const hyperid = require('hyperid')()
const Writable = streams.Writable
const ns = 'pubsub'
const cmd = 'subscribe'

function Receiver (mq, topic, key, peers) {
  this.mq = mq
  this.topic = topic
  this.key = key

  // used by mq the manage the number of
  // functions that are "listening" so we
  // can unsubscribe properly
  this.count = 1

  this.peers = peers

  this.streams = new Set()

  this.destroyed = false

  this.id = hyperid()

  this.logger = mq.upring.logger.child({
    receiver: {
      id: this.id,
      topic: topic,
      key: key
    }
  })

  if (peers) {
    mq.upring.on('peerUp', (peer) => {
      this._sendPeer(peer, 1, noop)
    })
  }
}

Receiver.prototype.send = function (done) {
  if (!this.destroyed) {
    if (this.peers) {
      this.logger.info({
        peers: this.peers
      }, 'subscribing to a group of peers')
      steed.each(this, this.peers, this._sendPeer, done)
    } else {
      this.logger.info({
        peers: this.peers
      }, 'subscribing via the hashring')

      this.mq.upring.request({
        cmd,
        ns,
        key: this.key,
        topic: this.topic,
        streams: {
          messages: this.stream(null, 0)
        }
      }, done)
    }
  } else {
    process.nextTick(done, new Error('already destroyed'))
  }
}

Receiver.prototype._sendPeer = function (peer, count, cb) {
  if (typeof count === 'function') {
    cb = count
    count = 1
  }

  if (count > 5) {
    // TODO we should probably log this, or do something
    // about it
    return
  }

  const conn = this.mq.upring.peerConn(peer)
  const req = {
    cmd,
    ns,
    topic: this.topic,
    streams: {
      messages: this.stream(peer, count)
    }
  }

  conn.request(req, cb)
}

Receiver.prototype.stream = function (peer, count) {
  const stream = new ReceiverStream(this, peer, count)
  this.streams.add(stream)
  return stream
}

Receiver.prototype.unsubscribe = function () {
  if (this.destroyed) {
    return
  }

  this.destroyed = true

  for (let stream in this.streams) {
    stream.source.destroy()
    // this should happen automatically
    // because tentacoli and upring use
    // pump
    // stream.end()
  }
}

function ReceiverStream (receiver, peer, count) {
  this.source = null
  this.receiver = receiver

  Writable.call(this, {
    objectMode: true
  })

  this.on('pipe', function (source) {
    receiver.logger.info('stream piped')
    this.source = source

    // can be called if we are unsubscribing
    // ad there is a response on the fly for the next
    // event loop run
    if (receiver.destroyed) {
      process.nextTick(source.destroy.bind(source))
    }

    // resubscribe if the stream closes
    const resubscribe = () => {
      receiver.logger.info('source stream closed, resubscribe')

      receiver.streams.delete(this)
      if (!receiver.destroyed) {
        if (this.peer) {
          receiver._sendPeer(this.peer, count + 1, noop)
        } else {
          receiver.send(noop)
        }
      }
    }

    eos(source, resubscribe)
  })
}

inherits(ReceiverStream, Writable)

ReceiverStream.prototype._writev = function (chunks, cb) {
  steed.each(this, chunks, processMsg, cb)
}

ReceiverStream.prototype._write = function (chunk, encoding, cb) {
  this.receiver.mq._internal.emit(chunk, cb)
}

function processMsg (entry, cb) {
  var chunk = entry.chunk
  this.receiver.logger.trace(chunk, 'delivering message')
  this.receiver.mq._internal.emit(chunk, cb)
}

function noop () {}

module.exports = Receiver
