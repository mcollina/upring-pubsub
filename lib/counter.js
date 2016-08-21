'use strict'

const maxInt = Math.pow(2, 31) - 1

function counter () {
  var current = 0

  return count

  function count () {
    var result = current
    if (current === maxInt) {
      current = 0
    } else {
      current += 1
    }
    return result
  }
}

module.exports = counter
module.exports.max = maxInt
