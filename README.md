# upring-pubsub

[![npm version][npm-badge]][npm-url]
[![Build Status][travis-badge]][travis-url]
[![Coverage Status][coveralls-badge]][coveralls-url]

PubSub system built on top of an [UpRing][upring] consistent hashring.
See [MQEmitter](http://github.com/mcollina/mqemitter) for the actual
API.

[![js-standard-style](https://raw.githubusercontent.com/feross/standard/master/badge.png)](https://github.com/feross/standard)

## Install

```
npm i upring-pubsub
```

## Usage

```js
'use strict'

const UpringPubsub = require('upring-pubsub')

const broker = UpringPubsub({
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
```

<a name="acknowledgements"></a>
## Acknowledgements

This project is kindly sponsored by [nearForm](http://nearform.com).

## License

MIT

[coveralls-badge]: https://coveralls.io/repos/github/mcollina/upring-pubsub/badge.svg?branch=master
[coveralls-url]: https://coveralls.io/github/mcollina/upring-pubsub?branch=master
[npm-badge]: https://badge.fury.io/js/upring-pubsub.svg
[npm-url]: https://badge.fury.io/js/upring-pubsub
[travis-badge]: https://api.travis-ci.org/mcollina/upring-pubsub.svg
[travis-url]: https://travis-ci.org/mcollina/upring-pubsub
[upring]: https://travis-ci.org/mcollina/upring
