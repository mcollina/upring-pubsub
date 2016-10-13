'use strict'

const UpRingPubSub = require('.')

const broker = UpRingPubSub({
  base: process.argv.slice(2)
})

broker.on('#', function (msg, cb) {
  console.log(msg)
  cb()
})

broker.upring.on('up', function () {
  console.log('copy and paste the following in a new terminal')
  console.log('node example', this.whoami())
})

var count = 0

setInterval(function () {
  count++
  broker.emit({
    topic: 'hello',
    count,
    payload: `from ${process.pid}`
  })
}, 1000)
