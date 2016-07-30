/* eslint no-undef: "off", no-native-reassign: "off" */

var netloc = '0.0.0.0'
if (typeof window === 'undefined') {
  expect = require('chai').expect
  PouchDB = require('pouchdb-node')
} else {
  expect = chai.expect
  process = {env: {}}
  netloc = location.hostname
}

var local
var remote = process.env.REMOTE_URL || 'http://' + netloc + ':5757/tiny-paw/db1'

describe('integration', function () {
  this.timeout(40000)

  before(function () {
    // cleaning up local db
    // remote doesn't need to be cleared as it should have been started out clear already
    return Promise.resolve().then(function () {
      return new PouchDB('pouch-test-db')
    }).then(function (db) {
      local = db
      return local.destroy()
    }).then(function () {
      return new PouchDB('pouch-test-db')
    }).then(function (db) {
      local = db
    })
  })

  describe('replication', function () {
    it('should replicate from remote to an empty local', function () {
      return Promise.resolve().then(function () {
        return local.replicate.from(remote)
      })
    })
  })
})
