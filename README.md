# UpRingPubSub

[![npm version][npm-badge]][npm-url]
[![Build Status][travis-badge]][travis-url]
[![Coverage Status][coveralls-badge]][coveralls-url]

PubSub system built on top of an [UpRing][upring] consistent hashring.  
You will get a pubsub system that is consistently available, so it can lose messages when the topology changes.  
However, it is massively scalable.

[![js-standard-style](https://raw.githubusercontent.com/feross/standard/master/badge.png)](https://github.com/feross/standard)

## Install

```
npm i upring-pubsub --save
```

## Usage

```js
const upring = require('upring')({
  base: process.argv.slice(2),
  hashring: {
    joinTimeout: 200
  }
})

upring.use(require('upring-pubsub'))

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
```

## API

### new UpRingPubSub(opts)

See [MQEmitter][mqemitter] for the actual API.

<a name="acknowledgements"></a>
## Acknowledgements

This project is kindly sponsored by [nearForm](http://nearform.com).

## License

MIT

[coveralls-badge]: https://coveralls.io/repos/github/upringjs/upring-pubsub/badge.svg?branch=master
[coveralls-url]: https://coveralls.io/github/upringjs/upring-pubsub?branch=master
[npm-badge]: https://badge.fury.io/js/upring-pubsub.svg
[npm-url]: https://badge.fury.io/js/upring-pubsub
[travis-badge]: https://api.travis-ci.org/upringjs/upring-pubsub.svg
[travis-url]: https://travis-ci.org/upringjs/upring-pubsub
[upring]: https://github.com/upringjs/upring
[mqemitter]: http://github.com/mcollina/mqemitter
