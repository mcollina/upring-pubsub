'use strict'

const test = require('tap').test
const UpRing = require('upring')
const UpringPubsub = require('..')

test('avvio lifecycle', t => {
  t.plan(2)
  const upring = UpRing({
    logLevel: 'error'
  })

  upring.use(UpringPubsub)

  upring.on('up', () => {
    t.ok(upring.isReady)
    t.ok(upring.pubsub)
    upring.close()
  })
})
