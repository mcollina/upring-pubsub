'use strict'

const streams = require('readable-stream')
const eos = require('end-of-stream')
const steed = require('steed')
const inherits = require('util').inherits
const hyperid = require('hyperid')()
const pump = require('pump')
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
    },
    serializers: {
      peers: peersSerializers
    }
  })

  if (peers) {
    mq.upring.on('peerUp', (peer) => {
      this.sendPeer(peer, 1, noop)
    })
  }
}

Receiver.prototype.send = function (done) {
  if (!this.destroyed) {
    if (this.peers) {
      this.logger.info({
        peers: this.peers
      }, 'subscribing to a group of peers')
      steed.each(this, this.peers, this.sendPeer, done)
    } else {
      this.logger.info({
        peers: this.peers
      }, 'subscribing via the hashring')

      this.mq.upring.request({
        cmd,
        ns,
        key: this.key,
        topic: this.topic,
        from: this.mq.whoami()
      }, (err, res) => {
        if (err) {
          return done(err)
        }

        if (!res.streams && !res.streams.messages) {
          return done(new Error('no messages stream'))
        }

        pump(res.streams.messages, this.stream(null, 0))
        done()
      })
    }
  } else {
    process.nextTick(done, new Error('already destroyed'))
  }
}

Receiver.prototype.sendPeer = function (peer, count, cb) {
  if (typeof count === 'function') {
    cb = count
    count = 1
  }

  if (count > 5) {
    // TODO we should probably log this, or do something
    // about it
    return
  }

  this.logger.debug({ peer, count }, 'direct subscribing')

  const conn = this.mq.upring.peerConn(peer)
  const req = {
    cmd,
    ns,
    topic: this.topic,
    from: this.mq.whoami(),
    streams: {
      messages: this.stream(peer, count)
    }
  }

  conn.request(req, (err, res) => {
    if (err) {
      return cb(err)
    }

    if (!res.streams && !res.streams.messages) {
      return cb(new Error('no messages stream'))
    }

    pump(res.streams.messages, this.stream(peer, count))
    cb()
  })
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

  // TODO this should go in a function
  // or event
  this.mq._receivers.delete(this)

  for (var stream in this.streams) {
    stream.source.destroy()
    stream.source.on('error', noop)
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
      source.on('error', noop)
      process.nextTick(source.destroy.bind(source))
      return
    }

    // resubscribe if the stream closes
    const resubscribe = () => {
      receiver.logger.info('source stream closed, resubscribe')

      receiver.streams.delete(this)
      if (!receiver.destroyed) {
        if (this.peer) {
          receiver.sendPeer(this.peer, count + 1, noop)
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

function peersSerializers (peers) {
  return peers ? peers.map(toId) : undefined
}

function toId (peer) { return peer.id }

module.exports = Receiver
