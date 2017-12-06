'use strict'

const tap = require('tap')
const max = 5
const UpRing = require('upring')
const UpringPubsub = require('..')
const steed = require('steed')
const maxInt = Math.pow(2, 32) - 1
const timeout = 6000
const joinTimeout = 1000
const logLevel = 'fatal'

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
  test('move a deep subscription', { timeout: timeout * 2 }, (t) => {
    t.plan(7)

    const another = UpRing({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    another.use(UpringPubsub, err => {
      if (err) throw err
    })
    t.tearDown(another.close.bind(another))

    const expected = {
      payload: { my: 'message' }
    }

    another.on('up', function () {
      const toKill = UpRing({
        base,
        logLevel,
        hashring: {
          joinTimeout
        }
      })
      toKill.use(UpringPubsub, err => {
        if (err) throw err
      })
      t.tearDown(toKill.close.bind(toKill))

      toKill.on('up', function () {
        let topic = 'hello/0'

        for (let i = 0; i < maxInt && !this.allocatedToMe(topic); i += 1) {
          topic = 'hello/' + i
        }
        // this topic is now allocated to toKill

        expected.topic = topic
        let count = 0
        let emitted = false

        const listener = (msg, cb) => {
          count++
          t.deepEqual(msg, expected, 'msg match')
          removeListener()
          cb()
        }

        function removeListener () {
          if (count === 2 && emitted) {
            another.pubsub.removeListener(topic, listener, function () {
              t.pass('removed listener')
            })
          }
        }

        let peerDown = 0
        let downPeer = false

        function emit () {
          if (peerDown === 2 && downPeer) {
            expected.payload = 'another'
            t.comment('emitting')
            setTimeout(function () {
              main.pubsub.emit(expected, function () {
                t.pass('emitted')
                emitted = true
                removeListener()
              })
            }, joinTimeout * 2)
          }
        }

        another.pubsub.on(topic, listener, (err) => {
          t.error(err)
          main.pubsub.emit(expected, function () {
            t.pass('emitted')

            another.on('peerDown', function () {
              t.comment('peerDown another')
              peerDown++
              emit()
            })

            another.on('peerDown', function () {
              t.comment('peerDown main')
              peerDown++
              emit()
            })

            toKill.close(function () {
              t.pass('closed')
              downPeer = true
              emit()
            })
          })
        })
      })
    })
  })
}
