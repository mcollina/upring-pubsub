'use strict'

const t = require('tap')
const UpringPubsub = require('..')
const joinTimeout = 2000
const maxInt = Math.pow(2, 32) - 1

const main = UpringPubsub({
  hashring: {
    joinTimeout
  }
})

t.tearDown(main.close.bind(main))

t.plan(7)

main.upring.on('up', () => {
  t.pass('main up')

  const peer = UpringPubsub({
    base: [main.whoami()],
    hashring: {
      joinTimeout
    }
  })

  t.tearDown(peer.close.bind(peer))

  let topic = 'hello/0'

  const expected = {
    payload: { my: 'message' }
  }

  peer.upring.on('up', function () {
    t.pass('peer up')

    // this is the main upring
    for (let i = 0; i < maxInt && main.upring.allocatedToMe(topic); i += 1) {
      topic = 'hello/' + i
    }

    // topic is allocated to the peer
    expected.topic = topic

    main.on('#', function (msg, cb) {
      t.deepEqual(msg, expected, 'msg match')
      cb()
    }, (err) => {
      t.error(err)

      main.on('hello/+', function (msg, cb) {
        t.deepEqual(msg, expected, 'msg match')
        cb()
      }, (err) => {
        t.error(err)

        main.emit(expected, function () {
          setTimeout(() => {
            t.pass('emitted')
          }, 1000)
        })
      })
    })
  })
})

