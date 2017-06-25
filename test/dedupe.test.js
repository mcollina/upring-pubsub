'use strict'

const t = require('tap')
const build = require('./helper').build
const maxInt = Math.pow(2, 32) - 1

const main = build()

t.tearDown(main.close.bind(main))

t.plan(7)

main.on('up', () => {
  t.pass('main up')

  const peer = build(main)

  t.tearDown(peer.close.bind(peer))

  let topic = 'hello/0'

  const expected = {
    payload: { my: 'message' }
  }

  peer.on('up', function () {
    t.pass('peer up')

    // this is the main upring
    for (let i = 0; i < maxInt && main.allocatedToMe(topic); i += 1) {
      topic = 'hello/' + i
    }

    // topic is allocated to the peer
    expected.topic = topic

    main.pubsub.on('#', function (msg, cb) {
      t.deepEqual(msg, expected, 'msg match 1')
      cb()
    }, (err) => {
      t.error(err)

      main.pubsub.on('hello/+', function (msg, cb) {
        t.deepEqual(msg, expected, 'msg match 2')
        cb()
      }, (err) => {
        t.error(err)

        main.pubsub.emit(expected, function () {
          setTimeout(() => {
            t.pass('emitted')
          }, build.joinTimeout * 2)
        })
      })
    })
  })
})
