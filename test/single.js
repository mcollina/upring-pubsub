'use strict'

const test = require('tap').test
const absTest = require('mqemitter/abstractTest')
const UpringPubsub = require('..')

absTest({
  builder: UpringPubsub,
  test
})
