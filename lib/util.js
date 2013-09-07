var r = require('rethinkdb')

var me = module.exports

me.clone = function(obj) {
  return JSON.parse(JSON.stringify(obj))
}

me.doesDBExist = function (callback) {
  var _this = this
  r.dbList().run(this.conn, function(err, dbs) {
    if (err) return callback(err)
    callback(null, dbs.indexOf(_this.db) >= 0)
  })
}

me.doTablesExist = function (callback) {
  var tablesToCheck = ['current', 'history']
  var _this = this
  r.db(_this.db).tableList().run(_this.conn, function(err, tables) {
    if (err) return callback(err)
    var dontExist = []
    tablesToCheck.forEach(function(table) {
      if (tables.indexOf(table) < 0)
        dontExist.push(table)
    })
    callback(null, dontExist)
  })
}
