'use strict'

const eos = require('end-of-stream')
const hyperid = require('hyperid')
const extractBase = require('./extractBase')

function load (pubsub) {
  const destSet = new Set()
  const idgen = hyperid()

  const tick = function () {
    destSet.clear()
  }

  // publish command
  pubsub.upring.add('ns:pubsub,cmd:publish', function (req, reply) {
    if (pubsub.closed) {
      reply(new Error('instance closing'))
      return
    }

    pubsub.logger.debug(req, 'emitting')
    pubsub._internal.emit(req.msg, reply)
  })

  // Subscribe command
  pubsub.upring.add('ns:pubsub,cmd:subscribe', function (req, reply) {
    const stream = req.streams && req.streams.messages
    const logger = pubsub.logger.child({
      incomingSubscription: {
        topic: req.topic,
        key: req.key,
        id: idgen()
      }
    })

    const dest = req.from

    if (!stream) {
      logger.warn('no stream')
      return reply(new Error('missing messages stream'))
    }

    if (pubsub.closed) {
      logger.warn('ring closed')
      stream.destroy()
      return reply(new Error('closing'))
    }

    const upring = pubsub.upring

    // the listener that we will pass to MQEmitter
    function listener (data, cb) {
      if (destSet.has(dest)) {
        logger.debug(data, 'duplicate data')
        // pubsub is a duplicate
        cb()
      } else if (!upring.allocatedToMe(extractBase(data.topic))) {
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

    logger.info('subscribe')

    if (req.key) {
      if (pubsub.upring.allocatedToMe(req.key)) {
        // close if the responsible peer changes
        // and trigger the reconnection mechanism on
        // the other side
        tracker = pubsub.upring.track(req.key)
        tracker.on('move', () => {
          logger.info('moving subscription to new peer')
          pubsub._internal.removeListener(req.topic, listener)
          if (stream.destroy) {
            stream.destroy()
          } else {
            stream.end()
          }
        })
      } else {
        logger.warn('unable to subscribe, not allocated here')
        pubsub._internal.removeListener(req.topic, listener)
        if (stream.destroy) {
          stream.destroy()
        } else {
          stream.end()
        }
      }
    }

    // remove the subscription when the stream closes
    eos(stream, () => {
      logger.info('stream closed')
      if (tracker) {
        tracker.end()
      }
      pubsub._internal.removeListener(req.topic, listener)
    })

    pubsub._internal.on(req.topic, listener, () => {
      // confirm the subscription
      reply()
    })
  })
}

module.exports = load
