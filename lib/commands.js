'use strict'

const eos = require('end-of-stream')
const hyperid = require('hyperid')
const extractBase = require('./extractBase')
const PassThrough = require('readable-stream').PassThrough
const hasLowWildcard = require('./hasLowWildcard')
const replicaSym = Symbol.for('replica')

function load (pubsub) {
  const destSet = new Set()
  const idgen = hyperid()
  const upring = pubsub.upring
  const internal = pubsub._internal

  const tick = function () {
    destSet.clear()
  }

  // publish command
  upring.add('ns:pubsub,cmd:publish', function (req, reply) {
    if (pubsub.closed) {
      reply(new Error('instance closing'))
      return
    }

    if (req.replica) {
      req.msg[replicaSym] = true
    }

    pubsub.logger.debug(req, 'emitting')
    internal.emit(req.msg, reply)

    if (req.key && upring.allocatedToMe(req.key) && !hasLowWildcard(req.msg.topic)) {
      const next = upring._hashring.next(req.key)

      if (next) {
        req.replica = true
        upring.logger.debug({ req, peer: next }, 'replicating publish')
        upring.peerConn(next).request(req, function (err) {
          if (err) {
            if (!pubsub.closed) {
              upring.logger.warn(err, 'error replicating')
            }
            return
          }
          upring.logger.debug(req, 'publish replicated')
        })
      }
    }
  })

  // Subscribe command
  upring.add('ns:pubsub,cmd:subscribe', function (req, reply) {
    const logger = pubsub.logger.child({
      incomingSubscription: {
        topic: req.topic,
        key: req.key,
        id: idgen()
      }
    })

    const dest = req.from

    if (pubsub.closed) {
      logger.warn('ring closed')
      return reply(new Error('closing'))
    }

    const stream = new PassThrough({ objectMode: true })
    const lowWildcard = hasLowWildcard(req.topic)

    // the listener that we will pass to MQEmitter
    function listener (data, cb) {
      if (destSet.has(dest) || lowWildcard && data[replicaSym]) {
        logger.debug(data, 'duplicate data')
        // message is a duplicate
        cb()
      } else if (!upring.allocatedToMe(extractBase(data.topic)) && !data[replicaSym]) {
        // nothing to do, we are not responsible for pubsub
        logger.debug(data, 'not allocated here')
        cb()
      } else {
        logger.debug(data, 'writing data')
        stream.write(data, cb)
        destSet.add(dest)
        // magically detect duplicates, as they will be emitted
        // in the same JS tick
        process.nextTick(tick)
      }
    }

    var tracker
    var replica
    var replicaPeer
    var needReply = true

    logger.info('subscribe')

    if (req.key) {
      if (upring.allocatedToMe(req.key)) {
        process.nextTick(createTracker)

        const next = upring._hashring.next(req.key)

        buildReplica(next, doReply)
        needReply = false
      } else {
        upring.replica(req.key, createTracker)
      }
    }

    if (needReply) {
      doReply()
    }

    // remove the subscription when the stream closes
    eos(stream, () => {
      logger.info('stream closed')
      if (tracker) {
        tracker.end()
      }
      internal.removeListener(req.topic, listener)
    })

    function doReply () {
      internal.on(req.topic, listener, () => {
        reply(null, { streams: { messages: stream } })
      })
    }

    function buildReplica (peer, cb) {
      cb = cb || noop

      if (!peer || pubsub.closed) {
        logger.info('stopping replication')
        cb()
        return
      }

      logger.info({ peer }, 'replicating subscription')

      if (replicaPeer && replicaPeer.id === peer.id) {
        return
      }

      replicaPeer = peer

      if (replica) {
        replica.destroy()
        replica = null
      }

      upring.peerConn(peer).request(req, function (err, res) {
        if (err) {
          if (!pubsub.closed) {
            logger.warn(err)
          }
          stream.end()
          cb()
          return
        }

        logger.info({ peer }, 'replication in place')

        internal.removeListener(req.topic, listener)

        res.streams.messages.pipe(stream, { end: false })

        cb()

        eos(res.streams.messages, function (err) {
          if (err) {
            logger.info({ peer }, 'replication stream died')
            return
          }

          const next = upring._hashring.next(req.key)
          buildReplica(next)
        })
      })
    }

    function createTracker () {
      // setup a replica
      tracker = upring.track(req.key, { replica: true })
      tracker.on('replica', buildReplica)
      tracker.on('move', function () {
        logger.info('removing subscription to new peer')
        internal.removeListener(req.topic, listener)
      })
      // TODO handle clase when we become the master again
    }
  })
}

function noop () {}

module.exports = load
