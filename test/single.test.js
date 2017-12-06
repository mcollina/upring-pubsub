'use strict'

const test = require('tap').test
const absTest = require('mqemitter/abstractTest')
const upring = require('upring')({
  logLevel: 'error'
})
const UpringPubsub = require('..')

upring.use(UpringPubsub)

upring.ready(() => {
  absTest({
    builder: function (opts) {
      return upring.pubsub
    },
    test
  })
})
