'use strict'

// Extract the base topic if there is a wildcard
function extractBase (topic) {
  const levels = topic.split('/')

  if (levels.length < 2) {
    return topic
  } else if (levels[1] === '#') {
    return levels[0]
  } else {
    return levels[0] + '/' + levels[1]
  }
}

module.exports = extractBase
