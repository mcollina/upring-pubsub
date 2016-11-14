'use strict'

// do we have a wildcard?
function hasLowWildCard (topic) {
  const levels = topic.split('/')

  return levels[0] === '#' || levels[1] === '#' ||
         levels[0] === '+' || levels[1] === '+'
}

module.exports = hasLowWildCard
