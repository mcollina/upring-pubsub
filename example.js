'use strict'

const upring = require('upring')({
  base: process.argv.slice(2),
  hashring: {
    joinTimeout: 200
  }
})

upring.use(require('./pubsub'))

var count = 0

upring.on('up', function () {
  console.log('copy and paste the following in a new terminal')
  console.log('node example', this.whoami())

  upring.pubsub.on('hello/world', function (msg, cb) {
    console.log(msg)
    cb()
  })

  setInterval(function () {
    count++
    upring.pubsub.emit({
      topic: 'hello/world',
      count,
      pid: process.pid
    })
  }, 1000)
})
