'use strict'

const tap = require('tap')
const max = 5
const UpringPubsub = require('..')
const steed = require('steed')
const maxInt = Math.pow(2, 32) - 1
const timeout = 6000
const joinTimeout = 500
const logLevel = 'fatal'

let peers = null
let base = null

const main = UpringPubsub({
  logLevel,
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
      logLevel,
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
  test('move a deep subscription', { timeout: timeout * 2 }, (t) => {
    t.plan(7)

    const another = UpringPubsub({
      base,
      logLevel,
      hashring: {
        joinTimeout
      }
    })
    t.tearDown(another.close.bind(another))

    const expected = {
      payload: { my: 'message' }
    }

    another.upring.on('up', function () {
      const toKill = UpringPubsub({
        base,
        logLevel,
        hashring: {
          joinTimeout
        }
      })
      t.tearDown(toKill.close.bind(toKill))

      toKill.upring.on('up', function () {
        let topic = 'hello/0'

        for (let i = 0; i < maxInt && !this.allocatedToMe(topic); i += 1) {
          topic = 'hello/' + i
        }

        expected.topic = topic
        let count = 0

        const listener = (msg, cb) => {
          count++
          t.deepEqual(msg, expected, 'msg match')
          if (count === 2) {
            another.removeListener(topic, listener, function () {
              t.pass('removed listener')
              cb()
            })
          } else {
            cb()
          }
        }

        let peerDown = 0
        let downPeer = false

        function emit () {
          if (peerDown === 2 && downPeer) {
            expected.payload = 'another'
            t.comment('emitting')
            setTimeout(function () {
              main.emit(expected, function () {
                t.pass('emitted')
              })
            }, joinTimeout * 2)
          }
        }

        another.on(topic, listener, (err) => {
          t.error(err)
          main.emit(expected, function () {
            t.pass('emitted')

            another.upring.on('peerDown', function () {
              t.comment('peerDown another')
              peerDown++
              emit()
            })

            another.upring.on('peerDown', function () {
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
