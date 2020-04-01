'use strict';

const driver = require("..");

const Code = require('@hapi/code');
const Lab = require('@hapi/lab');
const config = require("./db.config.json").mongodb;
const log = require("db-migrate-shared").log;
const dataType = require("db-migrate-shared").dataType;

const lab = exports.lab = Lab.script();
const expect = Code.expect;
const { describe, it } = lab;

const internals = {};
internals.mod = {
    log: log,
    type: dataType
};
internals.interfaces = {
    SeederInterface: {},
    MigratorInterface: {}
};
internals.migrationTable = "migrations";

let db

describe('db-migrate', () => {

    it('is connected', () => {
        const callback = (err, _db) => {
            expect(err).to.be.undefined;
            db = _db;
        }
        driver.connect(config, internals, callback)
    })

})