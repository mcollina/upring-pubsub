'use strict'

const test = require('tap').test
const absTest = require('mqemitter/abstractTest')
const max = 1
const UpringPubsub = require('..')
const steed = require('steed')

let peers = null
let base = null

function boot (done) {
  const main = UpringPubsub()

  main.upring.on('up', () => {
    let count = 0
    base = [main.whoami()]
    peers = [main]

    for (let i = 0; i < max; i++) {
      launch()
    }

    function launch () {
      const peer = UpringPubsub({
        base
      })
      peers.push(peer)
      peer.upring.on('up', () => {
        base.push(peer.whoami())
      })
      peer.upring.on('up', latch)
    }

    function latch () {
      if (++count === max) {
        done()
      }
    }
  })
}

function myTest (name, func) {
  test(name, (t) => {
    boot(() => {
      func(t)
    })
  })
}

absTest({
  builder: () => {
    const currentPeers = peers
    const instance = peers.shift()
    const oldClose = instance.close
    instance.close = function (done) {
      peers = null
      console.log('received close for peer', this.whoami())
      console.log('peers', currentPeers.map((peer) => peer.whoami()))
      steed.each(currentPeers, (peer, cb) => peer.close(cb), () => {
        console.log('calling close for current instance')
        oldClose.call(this, done)
      })
    }
    return instance
  },
  test: myTest
})
