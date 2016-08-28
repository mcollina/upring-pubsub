'use strict'

const UpRing = require('upring')
const inherits = require('util').inherits
const mqemitter = require('mqemitter')
const streams = require('readable-stream')
const eos = require('end-of-stream')
const steed = require('steed')
const counter = require('./lib/counter')
const Writable = streams.Writable
const ns = 'pubsub'
const reParallel = require('fastparallel')()

function UpRingPubSub (opts) {
  if (!(this instanceof UpRingPubSub)) {
    return new UpRingPubSub(opts)
  }

  this.upring = new UpRing(opts)
  this._internal = mqemitter(opts)

  this._streams = new Map()

  this._ready = false
  this.closed = false

  this.upring.add({
    ns,
    cmd: 'publish'
  }, (req, reply) => {
    if (this.closed) {
      reply(new Error('instance closing'))
      return
    }

    this._internal.emit(req.msg, reply)
  })

  const count = counter()

  var lastTick = 0
  var currTick = counter.max

  const tick = function () {
    currTick = count()
  }

  this._inboundStreams = new Set()

  this.upring.add({
    ns,
    cmd: 'subscribe'
  }, (req, reply) => {
    const stream = req.streams && req.streams.messages

    if (!stream) {
      return reply(new Error('missing messages stream'))
    }

    if (this.closed) {
      stream.end()
      return reply(new Error('closing'))
    }

    const upring = this.upring

    function listener (data, cb) {
      if (lastTick === currTick) {
        // this is a duplicate
        cb()
      } else if (!upring.allocatedToMe(extractBase(data.topic))) {
        // nothing to do, we are not responsible for this
        cb()
      } else {
        stream.write(data, cb)
        // magically detect duplicates, as they will be emitted
        // in the same JS tick
        lastTick = currTick
        process.nextTick(tick)
      }
    }

    this._inboundStreams.add(stream)

    // remove the subscription when the stream closes
    eos(stream, () => {
      this._inboundStreams.delete(stream)
      this._internal.removeListener(req.topic, listener)
    })

    this._internal.on(req.topic, listener, () => {
      // confirm the subscription
      reply()
    })
  })

  this.upring.on('up', () => {
    this._ready = true
  })
}

UpRingPubSub.prototype.whoami = function () {
  return this.upring.whoami()
}

function extractBase (topic) {
  const levels = topic.split('/')

  if (levels.length < 2) {
    return topic
  } else if (levels[1] === '#') {
    return levels[0]
  } else {
    return levels[0] + '/' + levels[1]
  }
}

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
  if (!this._ready) {
    this.upring.once('up', this.emit.bind(this, msg, cb))
    return
  }

  const key = extractBase(msg.topic)
  this.upring.request({
    cmd: 'publish',
    ns,
    key,
    msg
  }, cb || noop)
}

function Receiver (mq, topic, key, upring) {
  var that = this

  this._mq = mq
  this.count = 1
  Writable.call(this, {
    objectMode: true
  })

  // TODO avoid warning for now
  // refactor reconnect logic
  // to alloacate a new stream
  // for every reconnect
  this.setMaxListeners(0)

  this.on('pipe', function (source) {
    this.source = source

    if (this._destroyed) {
      process.nextTick(source.destroy.bind(source))
      return
    }

    eos(source, resubscribe)

    function resubscribe () {
      if (!that._destroyed) {
        if (key) {
          upring.request({
            ns,
            cmd: 'subscribe',
            topic,
            key,
            streams: {
              messages: that
            }
          }, resubscribe)
          return
        }
      }
    }
  })
}

inherits(Receiver, Writable)

Receiver.prototype.unsubscribe = function () {
  if (this._destroyed) {
    return
  }

  this._destroyed = true

  if (this.source) {
    this.source.destroy()
  }

  this.end()
}

Receiver.prototype._writev = function (chunks, cb) {
  reParallel(this, processMsg, chunks, cb)
}

Receiver.prototype._write = function (chunk, encoding, cb) {
  this._writev([{
    chunk,
    encoding,
    callback: noop
  }], cb)
}

function processMsg (entry, cb) {
  this._mq.emit(entry.chunk, cb)
}

UpRingPubSub.prototype.on = function (topic, onMessage, done) {
  if (!this._ready) {
    this.upring.once('up', this.on.bind(this, topic, onMessage, done))
    return
  }

  done = done || noop

  const key = extractBase(topic)
  if (!onMessage.__upWrap) {
    onMessage.__upWrap = (msg, cb) => {
      onMessage.call(this, msg, cb)
    }
  }

  let cmd = 'subscribe'

  this._internal.on(topic, onMessage.__upWrap)

  // data is already flowing through this instance
  // nothing to do
  if (this._streams.has(topic)) {
    this._streams.get(topic).count++
    done()
    return
  } else if (hasLowWildCard(topic)) {
    const members = this.upring._hashring.swim.members(false)
    const receiver = new Receiver(this._internal, topic, null, this.upring)
    this._streams.set(topic, receiver)

    receiver.setMaxListeners(0)

    const req = {
      cmd,
      ns,
      topic,
      streams: {
        messages: receiver
      }
    }

    steed.each(members, (peer, cb) => {
      let conn = this.upring.peerConn({
        id: peer.host,
        meta: peer.meta
      })
      conn.request(req, cb)
    }, done)
    return
  } else if (this.upring.allocatedToMe(key)) {
    // the message will be published here
    done()
    return
  }

  // normal case, we just need to go to a single instance
  const receiver = new Receiver(this._internal, topic, key, this.upring)
  this._streams.set(topic, receiver)

  this.upring.request({
    cmd,
    ns,
    key,
    topic,
    streams: {
      messages: receiver
    }
  }, done)
}

UpRingPubSub.prototype.removeListener = function (topic, onMessage, done) {
  const stream = this._streams.get(topic)

  if (stream && --stream.count === 0) {
    stream.unsubscribe()
    this._streams.delete(topic)
  }
  this._internal.removeListener(topic, onMessage.__upWrap, done)
}

UpRingPubSub.prototype.close = function (cb) {
  cb = cb || noop
  if (!this._ready) {
    this.upring.once('up', this.close.bind(this, cb))
    return
  }

  if (this.closed) {
    cb()
    return
  }

  this._streams.forEach((value, key) => {
    value.unsubscribe()
  })

  this.closed = true

  this._internal.close(() => {
    this.upring.close((err) => {
      cb(err)
    })
  })
}

function noop () {}

module.exports = UpRingPubSub
