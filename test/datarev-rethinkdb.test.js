var testutil = require('testutil')
  , dr = require('../lib/datarev-rethinkdb')
  , sutil = require('../lib/util')

var _db = null
  , DB_NAME = "TEST_datarev_rethinkdb"

describe('datarev-rethinkdb', function() {
  beforeEach(function(done) {
    _db = dr({db: DB_NAME})
    _db.connect(function(err) {
      F (err)
      _db.destroy(function(err) {
        _db.create(function(err) {
          done(err)
        })
      })
    })
  })

  describe('- create', function() {
    describe('> when db and tables do not exist', function() {
      it('should create the db and tables', function(done) {
        _db.destroy(function(err) {
          F (err)
          _db.create(function(err) {
            F (err)
            sutil.doesDBExist.call(_db, function(err, doesIt) {
              F (err)
              T (doesIt)
              sutil.doTablesExist.call(_db, function(err, tablesThatDontExist) {
                F (err)
                EQ (tablesThatDontExist.length, 0)
                done()
              })
            })
          })
        })
      })
    })
  })

  describe('- upsert', function() {
    describe('> when it doesnt exist', function() {
      it('should insert it', function(done) {
        var id = 1

        var obj = {
          original: {
            name: "Jon Paul"
          },
          current: {
            name: "JP"
          },
          revisions: [
            {p: ".name", old: "Jon Paul", val: "JP", m: 'R'}
          ],
          id: id
        }

        _db.upsert(obj, function(err) {
          F (err)
          _db.fetch(id, function(err, current, original, revisions) {
            F (err)
            EQ (current.name, obj.current.name)
            EQ (original.name, obj.original.name)
            EQ (revisions.length, 1)
            done()
          })
        })
      })
    })
  })
})

