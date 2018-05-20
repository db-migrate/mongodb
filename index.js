var util = require('util');
var moment = require('moment');
var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;
var Base = require('db-migrate-base');
var Promise = require('bluebird');
var log;
var type;

var MongodbDriver = Base.extend({

  init: function(connection, internals, mongoString) {
    this._super(internals);
    this.connection = connection;
    this.connectionString = mongoString;
  },

  /**
   * Creates the migrations collection
   *
   * @param callback
   */
  _createMigrationsCollection: function(callback) {
    // console.log('_createMigrationsCollection');
    return this._run('createCollection', this.internals.migrationTable, null)
      .nodeify(callback);
  },


  /**
   * Creates the seed collection
   *
   * @param callback
   */
  _createSeedsCollection: function(callback) {
    return this._run('createCollection', this.internals.seedTable, null)
      .nodeify(callback);
  },

  /**
   * An alias for _createMigrationsCollection
   */
  createMigrationsTable: function(callback) {
    // console.log('createMigrationsTable start');
    this._createMigrationsCollection(callback);
  },

  /**
   * An alias for _createSeederCollection
   */
  createSeedsTable: function(callback) {
    this._createSeedsCollection(callback);
  },

  /**
   * Creates a collection
   *
   * @param collectionName  - The name of the collection to be created
   * @param callback
   */
  createCollection: function(collectionName, callback) {
    return this._run('createCollection', collectionName, null)
      .nodeify(callback);
  },

  switchDatabase: function(options, callback) {

    if(typeof(options) === 'object')
    {
      if(typeof(options.database) === 'string')
        this._database = options.database;
    }
    else if(typeof(options) === 'string')
    {
      this._database = options;
    }

    return Promise.resolve().nodeify(callback);
  },

  createDatabase: function(dbName, options, callback) {
    //Don't care at all, MongoDB auto creates databases
    if(typeof(options) === 'function')
      callback = options;

    return Promise.resolve().nodeify(callback);
  },

  dropDatabase: function(dbName, options, callback) {

    if(typeof(options) === 'function')
      callback = options;

    return this._run('dropDatabase', dbName, null)
      .nodeify(callback);
  },

  /**
   * An alias for createCollection
   *
   * @param collectionName  - The name of the collection to be created
   * @param callback
   */
  createTable: function(collectionName, callback) {
    this.createCollection(collectionName, callback);
  },

  /**
   * Drops a collection
   *
   * @param collectionName  - The name of the collection to be dropped
   * @param callback
   */
  dropCollection: function(collectionName, callback) {
    return this._run('dropCollection', collectionName, null)
      .nodeify(callback);
  },

  /**
   * An alias for dropCollection
   *
   * @param collectionName  - The name of the collection to be dropped
   * @param callback
   */
  dropTable: function(collectionName, callback) {
    this.dropCollection(collectionName, callback);
  },

  /**
   * Renames a collection
   *
   * @param collectionName    - The name of the existing collection to be renamed
   * @param newCollectionName - The new name of the collection
   * @param callback
   */
  renameCollection: function(collectionName, newCollectionName, callback) {
    return this._run('renameCollection', collectionName, {newCollection: newCollectionName})
      .nodeify(callback);
  },

  /**
   * An alias for renameCollection
   *
   * @param collectionName    - The name of the existing collection to be renamed
   * @param newCollectionName - The new name of the collection
   * @param callback
   */
  renameTable: function(collectionName, newCollectionName, callback) {
    return this.renameCollection(collectionName, newCollectionName)
      .nodeify(callback);
  },

  /**
   * Adds an index to a collection
   *
   * @param collectionName  - The collection to add the index to
   * @param indexName       - The name of the index to add
   * @param columns         - The columns to add an index on
   * @param	unique          - A boolean whether this creates a unique index
   */
  addIndex: function(collectionName, indexName, columns, unique, callback) {

    var options = {
      indexName: indexName,
      columns: columns,
      unique: unique
    };

    return this._run('createIndex', collectionName, options)
      .nodeify(callback);
  },

  /**
   * Removes an index from a collection
   *
   * @param collectionName  - The collection to remove the index
   * @param indexName       - The name of the index to remove
   * @param columns
   */
  removeIndex: function(collectionName, indexName, callback) {
    return this._run('dropIndex', collectionName, {indexName: indexName})
      .nodeify(callback);
  },

  /**
   * Inserts a record(s) into a collection
   *
   * @param collectionName  - The collection to insert into
   * @param toInsert        - The record(s) to insert
   * @param callback
   */
  insert: function(collectionName, toInsert, callback) {
    return this._run('insert', collectionName, toInsert)
      .nodeify(callback);
  },

  /**
   * Inserts a migration record into the migration collection
   *
   * @param name                - The name of the migration being run
   * @param callback
   */
  addMigrationRecord: function (name, callback) {
    return this._run('insert', this.internals.migrationTable, {name: name, run_on: new Date()})
      .nodeify(callback);
  },

  /**
   * Inserts a seeder record into the seeder collection
   *
   * @param name                - The name of the seed being run
   * @param callback
   */
  addSeedRecord: function (name, callback) {
    return this._run('insert', this.internals.seedTable, {name: name, run_on: new Date()})
      .nodeify(callback);
  },
  
  /**
   * Returns the DB instance so custom updates can be made.
   * NOTE: This method exceptionally does not call close() on the database driver when the promise resolves. So the getDbInstance method caller
   * needs to call .close() on it's own after finish working with the database driver.
   *
   * @param callback with the database driver as 2nd callback argument
   */
  getDbInstance: function (callback) {
    return this._run('getDbInstance', null, {run_on: new Date()})
      .nodeify(callback);
  },

  /**
   * Runs a query
   *
   * @param collectionName  - The collection to query on
   * @param query           - The query to run
   * @param callback
   */
  _find: function(collectionName, query, callback) {
    return this._run('find', collectionName, query)
      .nodeify(callback);
  },

  /**
   * Gets all the collection names in mongo
   *
   * @param callback  - The callback to call with the collection names
   */
  _getCollectionNames: function(callback) {
    return this._run('collections', null, null)
      .nodeify(callback);
  },

  /**
   * Gets all the indexes for a specific collection
   *
   * @param collectionName  - The name of the collection to get the indexes for
   * @param callback        - The callback to call with the collection names
   */
  _getIndexes: function(collectionName, callback) {
    return this._run('indexInformation', collectionName, null)
      .nodeify(callback);
  },

  /**
   * Gets a connection and runs a mongo command and returns the results
   *
   * @param command     - The command to run against mongo
   * @param collection  - The collection to run the command on
   * @param options     - An object of options to be used based on the command
   * @param callback    - A callback to return the results
   */
  _run: function(command, collection, options, callback) {
    // console.log('_run', command, collection, options);

    var args = this._makeParamArgs(arguments),
        sort = null,
        callback = args[2];

    log.sql.apply(null, arguments);

    if(options && typeof(options) === 'object') {

      if(options.sort)
        sort = options.sort;
    }

    if(this.internals.dryRun) {
      return Promise.resolve().nodeify(callback);
    }

    return new Promise(function(resolve, reject) {
      var prCB = function(err, data) {
        return (err ? reject(err) : resolve(data));
      };

      // Get a connection to mongo
      this.connection.connect(this.connectionString, function(err, db) {
        // console.log('err', err);
        // console.log('db', db);

        if(err) {
          return prCB(err);
        }

        // Callback function to return mongo records
        var callbackFunction = function(err, data) {

          if(err) {
            prCB(err);
          }

          prCB(null, data);
          db.close();
        };

        // Depending on the command, we need to use different mongo methods
        switch(command) {
          case 'find':

            if(sort) {
              db.collection(collection)[command](options.query).sort(sort).toArray(callbackFunction);
            }
            else {
              db.collection(collection)[command](options).toArray(callbackFunction);
            }
            break;
          case 'renameCollection':
            db[command](collection, options.newCollection, callbackFunction);
            break;
          case 'createIndex':
            db[command](collection, options.columns, {name: options.indexName, unique: options.unique}, callbackFunction);
            break;
          case 'dropIndex':
            db.collection(collection)[command](options.indexName, callbackFunction);
            break;
          case 'insert':
            // options is the records to insert in this case
            if(util.isArray(options))
              db.collection(collection).insertMany(options, {}, callbackFunction);
            else
              db.collection(collection).insertOne(options, {}, callbackFunction);
            break;
          case 'remove':
            // options is the records to insert in this case
            if(util.isArray(options))
              db.collection(collection).deleteMany(options, callbackFunction);
            else
              db.collection(collection).deleteOne(options, callbackFunction);
            break;
          case 'collections':
            db.collections(callbackFunction);
            break;
          case 'indexInformation':
            db.indexInformation(collection, callbackFunction);
            break;
          case 'dropDatabase':
            db.dropDatabase(callbackFunction);
            break;
          case 'update':
            db.collection(collection)[command](options.query, options.update, options.options, callbackFunction);
            break;
          case 'updateMany':
            db.collection(collection)[command](options.query, options.update, options.options, callbackFunction);
            break;
          case 'getDbInstance':
            prCB(null, db); // When the user wants to get the DB instance we need to return the promise callback, so the DB connection is not automatically closed
            break;
          default:
            // console.log('db', db);
            // This should actually work but I think db is unexpectedly null
            db[command](collection, callbackFunction);
            break;
        }
      });
    }.bind(this)).nodeify(callback);
  },

  _makeParamArgs: function(args) {
    var params = Array.prototype.slice.call(args);
    var sql = params.shift();
    var callback = params.pop();

    if (params.length > 0 && Array.isArray(params[0])) {
      params = params[0];
    }

    return [sql, params, callback];
  },

  /**
   * Runs a NoSQL command regardless of the dry-run param
   */
  _all: function() {
    var args = this._makeParamArgs(arguments);
    return this.connection.query.apply(this.connection, args);
  },

  /**
   * Queries the migrations collection
   *
   * @param callback
   */
  allLoadedMigrations: function(callback) {
    return this._run('find', this.internals.migrationTable, { sort: { run_on: -1 } })
      .nodeify(callback);
  },

  /**
   * Queries the seed collection
   *
   * @param callback
   */
  allLoadedSeeds: function(callback) {
    return this._run('find', this.internals.seedTable, { sort: { run_on: -1 } })
      .nodeify(callback);
  },

  /**
   * Deletes a migration
   *
   * @param migrationName       - The name of the migration to be deleted
   * @param callback
   */
  deleteMigration: function(migrationName, callback) {
    return this._run('remove', this.internals.migrationTable, {name: migrationName})
      .nodeify(callback);
  },

  /**
   * Deletes a migration
   *
   * @param migrationName       - The name of the migration to be deleted
   * @param callback
   */
  deleteSeed: function(migrationName, callback) {
    return this._run('remove', this.internals.seedTable, {name: migrationName})
      .nodeify(callback);
  },

  /**
   * Closes the connection to mongodb
   */
  close: function(callback) {
    return Promise.resolve().nodeify(callback);
  },

  buildWhereClause: function() {

    return Promise.reject('There is no NoSQL implementation yet!');
  },

  update: function() {

    return Promise.reject('There is no NoSQL implementation yet!');
  }
});

Promise.promisifyAll(MongodbDriver);

function parseColonString( config, port, length ) {

  var result = '';

  for(var i = 0; i < length; ++i) {

    result += config.host[i] + ((config.host[i].indexOf(':') === -1) ?
      ':' + port : '') + ',';
  }

  return result.substring(0, result.length - 1);
}

function parseObjects( config, port, length ) {

  var result = '';

  for(var i = 0; i < length; ++i) {

    result += config.host[i].host + ((!config.host[i].port) ?
      ':' + port : ':' + config.host[i].port) + ',';
  }

  return result.substring(0, result.length - 1);
}

/**
 * Gets a connection to mongo
 *
 * @param config    - The config to connect to mongo
 * @param callback  - The callback to call with a MongodbDriver object
 */
exports.connect = function(config, intern, callback) {
  var db;
  var port;
  var host;

  var internals = intern;

  log = internals.mod.log;
  type = internals.mod.type;

  // Make sure the database is defined
  if(config.database === undefined) {
    throw new Error('database must be defined in database.json');
  }

  if(config.port === undefined) {
    port = 27017;
  } else {
    port = config.port;
  }

  config.host = util.isArray(config.hosts) ? config.hosts : config.host;

  if(config.host === undefined) {

    host = 'localhost' + ':' + port;
  } else if(util.isArray(config.host) ) {

    var length = config.host.length;
    host = '';

    if(typeof(config.host[0]) === 'string')
      host = parseColonString(config, port, length);
    else
      host = parseObjects(config, port, length);
  } else {

    host = config.host + ':' + port;
  }

  var mongoString = 'mongodb://';

  if(config.user !== undefined && config.password !== undefined) {
    mongoString += config.user + ':' + config.password + '@';
  }

  mongoString += host + '/' + config.database;

  var extraParams = [];
  if (config.ssl) {
    extraParams.push('ssl=true');
  }

  if(config.authSource !== undefined && config.user !== undefined && config.password !== undefined) {
    extraParams.push('authSource=' + config.authSource);
  }

  if (config.replicaSet){
    extraParams.push('replicaSet=' + config.replicaSet);
  }

  if (config.readPreference){
    extraParams.push('readPreference=' + config.readPreference);
  }

  if(extraParams.length > 0){
      mongoString += '?' + extraParams.join('&');
  }

  if (config.replSet) {
    var hosts = config.replSet.split(',');
    var servers = hosts.map(function(host) {
      return new Server(host, port, config.options);
    });
    db = config.db || new MongoClient(new ReplSet(servers, config.options));
  } else {
    db = config.db || new MongoClient(new Server(host, port));
  }

  callback(null, new MongodbDriver(db, intern, mongoString));

  // db = config.db || new MongoClient(new Server(host, port));
  // callback(null, new MongodbDriver(db, intern, mongoString));
};
