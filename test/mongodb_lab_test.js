const Code = require('@hapi/code');
const Lab = require('@hapi/lab');
var assert = require("assert");
var config = require("./db.config.json").mongodb;
var dataType = require("db-migrate-shared").dataType;
var dbmeta = require("db-meta");
var driver = require("../");
var log = require("db-migrate-shared").log;

const { expect } = Code;
const { after, before, describe, it } = exports.lab = Lab.script();

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

describe('mongodb', () => {
    before(() => {
        driver.connect(config, internals, (_err, client) => {
            db = client;
        });
    });

    after(() => {
    });

    it('has table metadata containing the event table', { timeout: 100000 }, async () => {
        await db.createCollection("event");
        const tables = await db._getCollectionNames();

        expect(tables.map(t => t.collectionName)).to.include("event");
    });
});
