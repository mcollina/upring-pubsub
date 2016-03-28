'use strict'

const test = require('tap').test
const absTest = require('mqemitter/abstractTest')
const max = 10
const UpringPubsub = require('.')

// this serves as a base node
// TODO use baseswim instead
const main = UpringPubsub()
const peers = [main]

main.upring.on('up', () => {
  let count = 0

  for (let i = 0; i < max; i++) {
    launch()
  }

  function launch () {
    const peer = UpringPubsub({
      base: [main.whoami()]
    })
    peers.push(peer)
    peer.upring.on('up', latch)
  }

  function latch () {
    if (++count === max) {
      absTest({
        builder: () => main,
        test: test
      })
    }
  }
})
