'use strict'

const test = require('tap').test
const UpRing = require('upring')
const UpringPubsub = require('..')

test('avvio lifecycle', t => {
  t.plan(4)
  const upring = UpRing({
    logLevel: 'error'
  })

  upring.use(UpringPubsub, err => {
    t.error(err)
    t.notOk(upring.isReady)
  })

  upring.on('up', () => {
    t.ok(upring.isReady)
    t.ok(upring.pubsub)
    upring.close()
  })
})
