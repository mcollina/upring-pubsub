'use strict'

const tap = require('tap')
const max = 5
const UpRing = require('upring')
const UpringPubsub = require('..')
const steed = require('steed')
const maxInt = Math.pow(2, 32) - 1
const timeout = 6000
const joinTimeout = 1000
const logLevel = 'error'

let peers = null
let base = null

const main = UpRing({
  logLevel,
  hashring: {
    joinTimeout
  }
})
main.use(UpringPubsub, err => {
  if (err) throw err
})

main.on('up', () => {
  let count = 0
  base = [main.whoami()]
  peers = [main]

  for (let i = 0; i < max; i++) {
    launch()
  }

  function launch () {
    const peer = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    peer.use(UpringPubsub, err => {
      if (err) throw err
    })
    peers.push(peer)
    peer.on('up', () => {
      base.push(peer.whoami())
    })
    peer.on('up', latch)
  }

  function latch () {
    if (++count === max) {
      tap.tearDown(() => {
        steed.each(peers, (peer, cb) => {
          peer.close(cb)
        })
      })
      start(tap.test)
    }
  }
})

function start (test) {
  test('pub sub on another instance', { timeout }, (t) => {
    t.plan(3)

    const instance = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    instance.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.on('up', function () {
      let topic = 'hello'

      // this is the instance upring
      for (let i = 0; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello' + i
      }

      expected.topic = topic

      instance.pubsub.on(topic, (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.pubsub.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('deep # wildcards', { timeout }, (t) => {
    t.plan(3)

    const instance = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    instance.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic + '/something'

      instance.pubsub.on(topic + '/+', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.pubsub.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('deep # wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    instance.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.pubsub.on(topic + '/#', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.pubsub.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('2nd level # wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    instance.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.pubsub.on('hello/#', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.pubsub.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('2nd level + wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    instance.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.pubsub.on('hello/+', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.pubsub.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('1st level # wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    instance.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.pubsub.on('#', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.pubsub.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('1st level + wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    instance.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.on('up', function () {
      let topic = '0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = '' + i
      }

      expected.topic = topic

      instance.pubsub.on('+', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.pubsub.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })
}
