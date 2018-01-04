var vows = require("vows");
var assert = require("assert");
var dbmeta = require("db-meta");
var dataType = require("db-migrate-shared").dataType;
var driver = require("../");
var log = require("db-migrate-shared").log;

var config = require("./db.config.json").mongodb;

var internals = {};
internals.mod = {
  log: log,
  type: dataType
};
internals.interfaces = {
  SeederInterface: {},
  MigratorInterface: {}
};
internals.migrationTable = "migrations";

var dbName = config.database;
var db;

vows
  .describe("mongodb")
  .addBatch({
    connect: {
      topic: function() {
        driver.connect(config, internals, this.callback);
      },

      "is connected": function(err, _db) {
        assert.isNull(err);
        db = _db;
      }
    }
  })
  .addBatch({
    createCollection: {
      topic: function() {
        db.createCollection("event", this.callback);
      },

      teardown: function() {
        db.dropCollection("event", this.callback);
      },

      "has table metadata": {
        topic: function() {
          db._getCollectionNames(this.callback);
        },

        "containing the event table": function(err, tables) {
          assert.equal(
            tables.filter(function(table) {
              return table.collectionName === "event";
            }).length,
            1
          );
        }
      }
    }
  })
  .addBatch({
    dropCollection: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }

            db.dropCollection("event", this.callback);
          }.bind(this)
        );
      },

      "has table metadata": {
        topic: function() {
          db._getCollectionNames(this.callback);
        },

        "containing no tables": function(err, tables) {
          assert.isNotNull(tables);
          assert.equal(
            tables.filter(function(table) {
              return table.collectionName === "event";
            }).length,
            0
          );
        }
      }
    }
  })
  .addBatch({
    renameCollection: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }

            db.renameCollection("event", "functions", this.callback);
          }.bind(this)
        );
      },

      teardown: function() {
        db.dropCollection("functions", this.callback);
      },

      "has table metadata": {
        topic: function() {
          db._getCollectionNames(this.callback);
        },

        "containing the functions table": function(err, tables) {
          var table = tables.filter(function(table) {
            return table.collectionName === "functions";
          });
          var index = 0;
          assert.isNotNull(tables);
          assert.equal(table.length, 1);

          if (tables[0].collectionName === "system.indexes") index = 1;

          assert.equal(tables[index].collectionName, "functions");
        }
      }
    }
  })
  .addBatch({
    addIndex: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }

            db.addIndex("event", "event_title", "title", false, this.callback);
          }.bind(this)
        );
      },

      teardown: function() {
        db.dropCollection("event", this.callback);
      },

      "preserves case": {
        topic: function() {
          db._getCollectionNames(this.callback);
        },

        "of the functions original table": function(err, tables) {
          var table = tables.filter(function(table) {
            return table.collectionName === "event";
          });
          var index = 0;
          assert.isNotNull(tables);
          assert.equal(table.length, 1);

          if (tables[0].collectionName === "system.indexes") index = 1;

          assert.equal(tables[index].collectionName, "event");
        }
      },

      "has resulting index metadata": {
        topic: function() {
          db._getIndexes("event", this.callback);
        },

        "with additional index": function(err, indexes) {
          assert.isDefined(indexes);
          assert.isNotNull(indexes);
          assert.include(indexes, "event_title");
        }
      }
    }
  })
  .addBatch({
    insertOne: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }
            db.insert(
              "event",
              { id: 2, title: "title" },
              function(err) {
                if (err) {
                  return this.callback(err);
                }

                db._find("event", { title: "title" }, this.callback);
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function() {
        db.dropCollection("event", this.callback);
      },

      "with additional row": function(err, data) {
        assert.equal(data.length, 1);
      }
    }
  })
  .addBatch({
    insertMany: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }
            db.insert(
              "event",
              [
                { id: 2, title: "title" },
                { id: 3, title: "lol" },
                { id: 4, title: "title" }
              ],
              function(err) {
                if (err) {
                  return this.callback(err);
                }

                db._find("event", { title: "title" }, this.callback);
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function() {
        db.dropCollection("event", this.callback);
      },

      "with additional row": function(err, data) {
        assert.equal(data.length, 2);
      }
    }
  })
  .addBatch({
    removeIndex: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }

            db.addIndex(
              "event",
              "event_title",
              "title",
              false,
              function(err, data) {
                if (err) {
                  return this.callback(err);
                }

                db.removeIndex("event", "event_title", this.callback);
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function() {
        db.dropCollection("event", this.callback);
      },

      "has resulting index metadata": {
        topic: function() {
          db._getIndexes("event", this.callback);
        },

        "without index": function(err, indexes) {
          if (err) {
            return this.callback(err);
          }

          assert.isDefined(indexes);
          assert.isNotNull(indexes);
          assert.notInclude(indexes, "event_title");
        }
      }
    }
  })
  .addBatch({
    createMigrationsTable: {
      topic: function() {
        db._createMigrationsCollection(this.callback);
      },

      teardown: function() {
        db.dropCollection("migrations", this.callback);
      },

      "has migrations table": {
        topic: function() {
          db._getCollectionNames(this.callback);
        },

        "has migrations table": function(err, tables) {
          var index = 0;
          assert.isNull(err);
          assert.isNotNull(tables);
          assert.equal(
            tables.filter(function(table) {
              return table.collectionName === "migrations";
            }).length,
            1
          );
          if (tables[0].collectionName === "system.indexes") index = 1;

          assert.equal(tables[index].collectionName, "migrations");
        }
      }
    }
  })
  .addBatch({
    removeIndex: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }

            db.addIndex(
              "event",
              "event_title",
              "title",
              false,
              function(err, data) {
                if (err) {
                  return this.callback(err);
                }

                db.removeIndex("event", "event_title", this.callback);
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function() {
        db.dropCollection("event", this.callback);
      },

      "has resulting index metadata": {
        topic: function() {
          db._getIndexes("event", this.callback);
        },

        "without index": function(err, indexes) {
          if (err) {
            return this.callback(err);
          }

          assert.isDefined(indexes);
          assert.isNotNull(indexes);
          assert.notInclude(indexes, "event_title");
        }
      }
    }
  })
  .addBatch({
    createMigrationsTable: {
      topic: function() {
        db._createMigrationsCollection(this.callback);
      },

      teardown: function() {
        db.dropCollection("migrations", this.callback);
      },

      "has migrations table": {
        topic: function() {
          db._getCollectionNames(this.callback);
        },

        "has migrations table": function(err, tables) {
          var index = 0;
          assert.isNull(err);
          assert.isNotNull(tables);
          assert.equal(
            tables.filter(function(table) {
              return table.collectionName === "migrations";
            }).length,
            1
          );

          if (tables[0].collectionName === "system.indexes") index = 1;

          assert.equal(tables[index].collectionName, "migrations");
        }
      }
    }
  })
  .addBatch({
    updateMany: {
      topic: function() {
        db.createCollection(
          "event",
          function(err, collection) {
            if (err) {
              return this.callback(err);
            }
            db.insert(
              "event",
              { id: 2, title: "title" },
              function(err) {
                if (err) {
                  return this.callback(err);
                }

                var command = "updateMany";
                var renameFieldOptions = {
                  query: {},
                  update: {
                    $rename: {
                      title: "titleUpdated"
                    }
                  },
                  options: {}
                };

                db._run(
                  command,
                  "event",
                  renameFieldOptions,
                  function(err) {
                    if (err) {
                      return this.callback(err);
                    }
                    db._find("event", { titleUpdated: "title" }, this.callback);
                  }.bind(this)
                );
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function() {
        db.dropCollection("event", this.callback);
      },

      "with renamed row": function(err, data) {
        assert.equal(data.length, 1);
      }
    }
  })
  .addBatch({
    getDbInstance: {
      topic: function() {
        db.getDbInstance(this.callback);
      },

      teardown: function() {
      },

      "has database reference": function(err, db) {
        assert.isNotNull(db);
      }
    }
  })
  .export(module);
