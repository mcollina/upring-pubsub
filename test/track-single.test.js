'use strict'

const t = require('tap')
const farmhash = require('farmhash')
const build = require('./helper').build
const joinTimeout = build.joinTimeout

function getKey (one, two) {
  const onePoints = one._hashring.mymeta().points
  var start = onePoints[0]
  var end = 0
  const twoPoints = two._hashring.mymeta().points
  var i

  for (i = 0; i < twoPoints.length; i++) {
    if (twoPoints[i] > start) {
      end = twoPoints[i]
      for (var k = 1; k < onePoints.length; k++) {
        if (onePoints[k] > end) {
          start = onePoints[k - 1]
          break
        }
      }
      if (start > onePoints[0]) {
        break
      }
    }
  }

  var key
  var hash
  i = 0

  do {
    key = 'hello/' + i++
    hash = farmhash.hash32(key)
  } while (!(start < hash && hash < end))

  return key
}

t.test('from A to B', (t) => {
  const main = build()
  t.tearDown(main.close.bind(main))

  t.plan(5)

  main.on('up', () => {
    t.pass('main up')

    const peer = build()
    t.tearDown(peer.close.bind(peer))

    peer.on('up', function () {
      t.pass('peer up')

      // the topic responsibility will move from
      // main to peer
      const topic = getKey(main, peer)

      const expected = {
        topic,
        payload: { my: 'message' }
      }

      main.pubsub.on(topic, function (msg, cb) {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)

        peer.join([main.whoami()])
      })

      main.on('peerUp', function () {
        setTimeout(function () {
          main.pubsub.emit(expected, function () {
            t.pass('emitted')
          })
        }, joinTimeout)
      })
    })
  })
})

t.test('from B to A', (t) => {
  const main = build()

  t.tearDown(main.close.bind(main))

  t.plan(5)

  main.on('up', () => {
    t.pass('main up')

    const peer = build()

    t.tearDown(peer.close.bind(peer))

    peer.on('up', function () {
      t.pass('peer up')

      // the topic responsibility will stay in
      // main, as peer gets online
      const topic = getKey(peer, main)

      const expected = {
        topic,
        payload: { my: 'message' }
      }

      main.pubsub.on(topic, function (msg, cb) {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)
        peer.join([main.whoami()])
      })

      main.on('peerUp', function () {
        setTimeout(function () {
          main.pubsub.emit(expected, function () {
            t.pass('emitted')
          })
        }, joinTimeout)
      })
    })
  })
})

t.test('from A -> B, to A -> C', (t) => {
  const main = build()

  t.tearDown(main.close.bind(main))

  t.plan(6)

  main.on('up', () => {
    t.pass('main up')

    const peer = build(main)

    t.tearDown(peer.close.bind(peer))

    peer.on('up', function () {
      t.pass('peer up')

      const peer2 = build()

      t.tearDown(peer2.close.bind(peer2))

      peer2.on('up', function () {
        t.pass('peer up')

        const topic = getKey(peer, peer2)

        const expected = {
          topic,
          payload: { my: 'message' }
        }

        main.pubsub.on(topic, function (msg, cb) {
          t.deepEqual(msg, expected, 'msg match')
          cb()
        }, (err) => {
          t.error(err)

          peer2.join([main.whoami()])
        })

        main.on('peerUp', function () {
          setTimeout(function () {
            main.pubsub.emit(expected, function () {
              t.pass('emitted')
            })
          }, joinTimeout)
        })
      })
    })
  })
})
