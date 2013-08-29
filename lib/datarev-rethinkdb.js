var r = require('rethinkdb')
  , sutil = require('./util')

module.exports = function(params) {
  return new DataRevDB(params)
}


function DataRevDB (params) {
  this._opts = {}
  this._opts.host = params.host || 'localhost'
  this._opts.port = params.port || 28015
  
  if (!params.db) throw new Error("Must specify 'db' parameter.")
  this._opts.db = params.db
  this.db = params.db

  if (params.authKey) this._opts.authKey = params.authKey

  this.conn = null
  this.primaryKey = 'id'
}

DataRevDB.prototype.connect = function(callback) {
  var _this = this
  r.connect(this._opts, function(err, conn) {
    if (err) return callback(err)
    _this.conn = conn
    callback(null)
  })
}

DataRevDB.prototype.close = function() {
  this.connection.close()
}

DataRevDB.prototype.create = function(callback) {
  var _this = this
  sutil.doesDBExist.call(this, function(err, dbExists) {
    if (err) return callback(err)
    if (dbExists) 
      return checkTables()
    else
      r.dbCreate(_this.db).run(_this.conn, function(err, res) {
        if (err) return callback(err)
        if (!res.created) return callback(new Error("Database did not return an error, but didn't create the database named " + _this.db))
        checkTables()
      })
  })

  function checkTables () {
    sutil.doTablesExist.call(_this, function(err, tablesThatDontExist) {
      if (tablesThatDontExist.length === 0) 
        return callback(null)
      
      function createIt (table) {
        r.db(_this.db).tableCreate(table, {primaryKey: _this.primaryKey}).run(_this.conn, function(err, res) {
          if (err) return callback(err)
          if (!res.created) return callback(new Error("Database did not return an error, but didn't create the table named " + table))

          if (tablesThatDontExist.length === 0)
            return callback(null)
          else
            createIt(tablesThatDontExist.pop())
        })
      }
      createIt(tablesThatDontExist.pop())
    })
  }
}

DataRevDB.prototype.destroy = function(callback) {
  var _this = this
  r.dbDrop(this._opts.db).run(this.conn, function(err, res) {
    if (err) return callback(err)
    if (!res.dropped) return call(new Error("Database did not return an error, but didn't drop the database named " + _this.db)) 
    callback(null)
  })
}

DataRevDB.prototype.fetch = function(id, callback) {
  var _this = this
  r.table('currentRecords').get(id).run(this.conn, function(err, current) {
    if (err) return callback(err)
    r.table('originalRecords').get(id).run(_this.conn, function(err, original) {
      if (err) return callback(err)
      r.table('revisions').get(id).run(_this.conn, function(err, revisions) {
        if (err) return callback(err)
        delete current.id
        delete original.id
        delete revisions.id
        callback(null, current, original, revisions.revisions)
      })
    })
  })
}

DataRevDB.prototype.upsert = function(drObj, callback) {
  var drObj = sutil.clone(drObj)
  drObj.current.id = drObj.id
  drObj.original.id = drObj.id

  var revisions = {
    id: drObj.id,
    revisions: drObj.revisions
  }

  var _this = this
  r.table('currentRecords').insert(drObj.current, {upsert: true}).run(this.conn, function(err, res) {
    if (err) return callback(err)
    r.table('originalRecords').insert(drObj.original, {upsert: true}).run(_this.conn, function(err, res) {
      if (err) return callback(err)
      r.table('revisions').insert(revisions, {upsert: true}).run(_this.conn, function(err, res) {
        if (err) return callback(err)
        callback(null)
      })
    })
  })
}





