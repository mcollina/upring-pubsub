'use strict'

const UpRing = require('upring')
const UpringPubsub = require('..')
const joinTimeout = 200

function build (main) {
  const base = []

  if (main && main.whoami) {
    base.push(main.whoami())
  }

  const upring = UpRing({
    base,
    logLevel: 'error',
    hashring: {
      joinTimeout
    }
  })

  upring.use(UpringPubsub, err => {
    if (err) throw err
  })

  return upring
}

build.joinTimeout = joinTimeout

module.exports.build = build
