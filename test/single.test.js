// Adapted from https://github.com/mcollina/mqemitter/blob/master/abstractTest.js

/*
 * Copyright (c) 2014-2017, Matteo Collina <hello@matteocollina.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR
 * IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

'use strict'

const test = require('tap').test
const Upring = require('upring')
const UpringPubsub = require('../pubsub')
var Buffer = require('safe-buffer').Buffer

test('support on and emit', function (t) {
  t.plan(4)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello world', function (message, cb) {
      t.equal(upring.pubsub.current, 1, 'number of current messages')
      t.deepEqual(message, expected)
      t.equal(this, upring.pubsub)
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.pass('closed')
        })
      })
    })
  })
})

test('support multiple subscribers', function (t) {
  t.plan(3)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello world', function (message, cb) {
      t.ok(message, 'message received')
      cb()
    }, onFirstSubscribe)

    function onFirstSubscribe () {
      upring.pubsub.on('hello world', function (message, cb) {
        t.ok(message, 'message received')
        cb()
      }, onSecondSubscribe)
    }

    function onSecondSubscribe () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.ok('closed')
        })
      })
    }
  })
})

test('support multiple subscribers and unsubscribers', function (t) {
  t.plan(2)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello world',
    payload: { my: 'message' }
  }

  function first (message, cb) {
    t.fail('first listener should not receive any events')
    cb()
  }

  function second (message, cb) {
    t.ok(message, 'second listener must receive the message')
    cb()
    upring.close(function () {
      t.pass('closed')
    })
  }

  upring.on('up', () => {
    upring.pubsub.on('hello world', first, function () {
      upring.pubsub.on('hello world', second, function () {
        upring.pubsub.removeListener('hello world', first, function () {
          upring.pubsub.emit(expected)
        })
      })
    })
  })
})

test('removeListener', function (t) {
  t.plan(1)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello world',
    payload: { my: 'message' }
  }
  var toRemoveCalled = false

  function toRemove (message, cb) {
    toRemoveCalled = true
    cb()
  }

  upring.on('up', () => {
    upring.pubsub.on('hello world', function (message, cb) {
      cb()
    }, function () {
      upring.pubsub.on('hello world', toRemove, function () {
        upring.pubsub.removeListener('hello world', toRemove, function () {
          upring.pubsub.emit(expected, function () {
            upring.close(function () {
              t.notOk(toRemoveCalled, 'the toRemove function must not be called')
            })
          })
        })
      })
    })
  })
})

test('without a callback on emit and on', function (t) {
  t.plan(1)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello world', function (message, cb) {
      cb()
      upring.close(function () {
        t.pass('closed')
      })
    })

    setTimeout(function () {
      upring.pubsub.emit(expected)
    }, 100)
  })
})

test('without any listeners', function (t) {
  t.plan(2)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.emit(expected)
    t.equal(upring.pubsub.current, 0, 'reset the current messages trackers')
    upring.close(function () {
      t.pass('closed')
    })
  })
})

test('support one level wildcard', function (t) {
  t.plan(2)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello/world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello/+', function (message, cb) {
      t.equal(message.topic, 'hello/world')
      cb()
    }, function () {
      // this will not be catched
      upring.pubsub.emit({ topic: 'hello/my/world' })

      // this will be catched
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.pass('closed')
        })
      })
    })
  })
})

test('support changing one level wildcard', function (t) {
  t.plan(2)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub, { wildcardOne: '~' })

  var expected = {
    topic: 'hello/world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello/~', function (message, cb) {
      t.equal(message.topic, 'hello/world')
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.pass('closed')
        })
      })
    })
  })
})

test('support deep wildcard', function (t) {
  t.plan(2)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello/my/world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello/#', function (message, cb) {
      t.equal(message.topic, 'hello/my/world')
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.pass('closed')
        })
      })
    })
  })
})

test('support changing deep wildcard', function (t) {
  t.plan(2)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub, { wildcardSome: '*' })

  var expected = {
    topic: 'hello/my/world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello/*', function (message, cb) {
      t.equal(message.topic, 'hello/my/world')
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.pass('closed')
        })
      })
    })
  })
})

test('support changing the level separator', function (t) {
  t.plan(2)

  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub, { separator: '~' })

  var expected = {
    topic: 'hello~world',
    payload: { my: 'message' }
  }

  upring.on('up', () => {
    upring.pubsub.on('hello~+', function (message, cb) {
      t.equal(message.topic, 'hello~world')
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.pass('closed')
        })
      })
    })
  })
})

test('close support', function (t) {
  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var check = false

  upring.on('up', () => {
    t.notOk(upring.closed, 'must have a false closed property')

    upring.close(function () {
      t.ok(check, 'must delay the close callback')
      t.ok(upring.pubsub.closed, 'must have a true closed property')
      t.end()
    })

    check = true
  })
})

test('emit after close errors', function (t) {
  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  upring.on('up', () => {
    upring.close(function () {
      upring.pubsub.emit({ topic: 'hello' }, function (err) {
        t.ok(err, 'must return an error')
        t.end()
      })
    })
  })
})

test('support multiple subscribers with wildcards', function (t) {
  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello/world',
    payload: { my: 'message' }
  }
  var firstCalled = false
  var secondCalled = false

  upring.on('up', () => {
    upring.pubsub.on('hello/#', function (message, cb) {
      t.notOk(firstCalled, 'first subscriber must only be called once')
      firstCalled = true
      cb()
    })

    upring.pubsub.on('hello/+', function (message, cb) {
      t.notOk(secondCalled, 'second subscriber must only be called once')
      secondCalled = true
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.end()
        })
      })
    })
  })
})

test('support multiple subscribers with wildcards (deep)', function (t) {
  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var expected = {
    topic: 'hello/my/world',
    payload: { my: 'message' }
  }
  var firstCalled = false
  var secondCalled = false

  upring.on('up', () => {
    upring.pubsub.on('hello/#', function (message, cb) {
      t.notOk(firstCalled, 'first subscriber must only be called once')
      firstCalled = true
      cb()
    })

    upring.pubsub.on('hello/+/world', function (message, cb) {
      t.notOk(secondCalled, 'second subscriber must only be called once')
      secondCalled = true
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.end()
        })
      })
    })
  })
})

test('emit & receive buffers', function (t) {
  const upring = Upring({ logLevel: 'silent' })
  upring.use(UpringPubsub)

  var msg = Buffer.from('hello')
  var expected = {
    topic: 'hello',
    payload: msg
  }

  upring.on('up', () => {
    upring.pubsub.on('hello', function (message, cb) {
      t.deepEqual(msg, message.payload)
      cb()
    }, function () {
      upring.pubsub.emit(expected, function () {
        upring.close(function () {
          t.end()
        })
      })
    })
  })
})
