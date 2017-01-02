/* eslint no-undef: "off", no-native-reassign: "off" */

var netloc = '0.0.0.0'
if (typeof window === 'undefined') {
  expect = require('chai').expect
  PouchDB = require('pouchdb-node')
  fetch = require('node-fetch')
} else {
  expect = chai.expect
  process = {env: {}}
  netloc = location.hostname
}

var local
var remote = process.env.REMOTE_URL || 'http://' + netloc + ':5757/tiny-paw/db1'
var remoteApp = process.env.REMOTE_APP_URL || 'https://tiny-paw.hyperdev.space'
// this test/demo replicates with the app at https://hyperdev.com/#!/project/tiny-paw
// which starts its database with some data.
// also, this demo supposes the main database for this app has a record
// pointing to the target app.

describe('integration', function () {
  this.timeout(40000)

  before(function () {
    // cleaning up local db
    // remote doesn't need to be cleared as it should have been started out clear already
    return Promise.resolve().then(function () {
      return new PouchDB('test')
    }).then(function (db) {
      local = db
      return local.destroy()
    }).then(function () {
      return new PouchDB('test')
    }).then(function (db) {
      local = db
    })
  })

  describe('replication', function () {
    var match

    it('should replicate from remote to an empty local', function () {
      return Promise.resolve()
        .then(() => local.replicate.from(remote))
        .then(res => {
          expect(res.doc_write_failures).to.equal(0)
          expect(res.ok).to.be.true

          return local.allDocs({
            keys: ['teams/ytrueioc', 'matches/szpwuuer32'], include_docs: true
          })
        })
        .then((res) => {
          expect(res.rows[0].doc.name).to.equal('Blazing Brizzles')
          expect(res.rows[1].doc.score).to.equal('0-1')

          match = res.rows[1].doc
        })
    })
    it('should replicate after changing docs locally', function () {
      return Promise.resolve()
        .then(() => local.put({_id: 'teams/lsoeucs', name: 'Landing Lunars'}))
        .then(() => local.replicate.to(remote))
        .then(res => {
          expect(res.ok).to.be.true
          expect(res.docs_written).to.equal(1)

          return fetch(remoteApp + '/api/db1/teams/lsoeucs')
            .then(r => r.json())
        })
        .then(res => {
          delete res.last_update
          expect(res).to.deep.equal({'id': 'teams/lsoeucs', 'name': 'Landing Lunars'})

          match.score = '1-1'
          return local.put(match)
        })
        .then(res => local.replicate.to(remote))
        .then(() => fetch(remoteApp + '/api/db1/' + match._id).then(r => r.json()))
        .then(res => {
          delete res.last_update
          expect(res).to.deep.equal(match)
        })
    })
  })
})
