'use strict'

const UpringPubsub = require('..')
const joinTimeout = 200

function build (main) {
  const base = []

  if (main && main.whoami) {
    base.push(main.whoami())
  }

  return UpringPubsub({
    base,
    logLevel: 'error',
    hashring: {
      joinTimeout
    }
  })
}

build.joinTimeout = joinTimeout

module.exports.build = build
