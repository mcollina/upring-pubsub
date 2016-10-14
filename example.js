'use strict'

const UpRingPubSub = require('.')

const broker = UpRingPubSub({
  base: process.argv.slice(2)
})

var count = 0

broker.upring.on('up', function () {
  console.log('copy and paste the following in a new terminal')
  console.log('node example', this.whoami())

  broker.on('hello/world', function (msg, cb) {
    console.log(msg)
    cb()
  })

  setInterval(function () {
    count++
    broker.emit({
      topic: 'hello/world',
      count,
      payload: `from ${process.pid}`
    })
  }, 1000)
})
