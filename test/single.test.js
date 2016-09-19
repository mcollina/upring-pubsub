'use strict'

const test = require('tap').test
const absTest = require('mqemitter/abstractTest')
const UpringPubsub = require('..')

absTest({
  builder: function (opts) {
    opts = opts || {}
    opts.logLevel = 'error'
    return UpringPubsub(opts)
  },
  test
})
