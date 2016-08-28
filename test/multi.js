'use strict'

const tap = require('tap')
const max = 5
const UpringPubsub = require('..')
const steed = require('steed')
const maxInt = Math.pow(2, 32) - 1
const timeout = 6000
const joinTimeout = 2000

let peers = null
let base = null

const main = UpringPubsub({
  hashring: {
    joinTimeout
  }
})

main.upring.on('up', () => {
  let count = 0
  base = [main.whoami()]
  peers = [main]

  for (let i = 0; i < max; i++) {
    launch()
  }

  function launch () {
    const peer = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    peers.push(peer)
    peer.upring.on('up', () => {
      base.push(peer.whoami())
    })
    peer.upring.on('up', latch)
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

    const instance = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.upring.on('up', function () {
      let topic = 'hello'

      // this is the instance upring
      for (let i = 0; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello' + i
      }

      expected.topic = topic

      instance.on(topic, (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('deep # wildcards', { timeout }, (t) => {
    t.plan(3)

    const instance = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.upring.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic + '/something'

      instance.on(topic + '/+', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('deep # wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.upring.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.on(topic + '/#', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('2nd level # wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.upring.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.on('hello/#', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('2nd level + wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.upring.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.on('hello/+', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('1st level # wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.upring.on('up', function () {
      let topic = 'hello/0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = 'hello/' + i
      }

      expected.topic = topic

      instance.on('#', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('1st level + wildcard', { timeout }, (t) => {
    t.plan(3)

    const instance = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(instance.close.bind(instance))

    const expected = {
      payload: { my: 'message' }
    }

    instance.upring.on('up', function () {
      let topic = '0'

      for (let i = 1; i < maxInt && this.allocatedToMe(topic); i += 1) {
        topic = '' + i
      }

      expected.topic = topic

      instance.on('+', (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        instance.emit(expected, function () {
          t.pass('emitted')
        })
      })
    })
  })

  test('move a deep subscription', { timeout: timeout * 2 }, (t) => {
    t.plan(6)

    const toKill = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(toKill.close.bind(toKill))

    const another = UpringPubsub({
      base,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(another.close.bind(another))

    const expected = {
      payload: { my: 'message' }
    }

    toKill.upring.on('up', function () {
      let topic = 'hello'

      for (let i = 0; i < maxInt && !this.allocatedToMe(topic); i += 1) {
        topic = 'hello' + i
      }

      expected.topic = topic

      another.on(topic, (msg, cb) => {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        main.emit(expected, function () {
          t.pass('emitted')

          toKill.close(function () {
            t.pass('closed')

            setTimeout(function () {
              expected.payload = 'another'
              main.emit(expected, function () {
                t.pass('emitted')
              })
            }, 2000)
          })
        })
      })
    })
  })
}
