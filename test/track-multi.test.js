'use strict'

const test = require('tap').test
const UpringPubsub = require('./helper').build
const joinTimeout = UpringPubsub.joinTimeout * 5
const timeout = joinTimeout * 6

// returns a key allocated to the passed instance
function getKey (instance) {
  let key = 'hello'

  while (!instance.allocatedToMe(key)) {
    key += '1'
  }

  return key
}

test('# subscription', { timeout }, (t) => {
  const main = UpringPubsub()
  t.tearDown(main.close.bind(main))

  t.plan(7)

  main.upring.on('up', () => {
    t.pass('main up')

    let expected = null

    main.on('#', function (msg, cb) {
      t.deepEqual(msg, expected, 'msg match')
      cb()
    }, (err) => {
      t.error(err)

      const peer = UpringPubsub(main)
      t.tearDown(peer.close.bind(peer))
      let peerUp = false
      let upPeer = false

      function emit () {
        if (peerUp && upPeer) {
          const topic = getKey(peer.upring)

          expected = {
            topic,
            payload: { my: 'message' }
          }

          t.pass('emitting')

          // needed to allow the connection
          // to establish
          setImmediate(function () {
            peer.emit(expected, function () {
              t.pass('emitted')
            })
          })
        }
      }

      main.upring.on('peerUp', function () {
        t.pass('peerUp')
        peerUp = true
        emit()
      })

      peer.upring.on('up', function () {
        t.pass('upPeer')
        upPeer = true
        emit()
      })
    })
  })
})
