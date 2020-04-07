'use strict';

const driver = require('..');

const Code = require('@hapi/code');
const Lab = require('@hapi/lab');
const config = require('./db.config.json').mongodb;
const log = require('db-migrate-shared').log;
const dataType = require('db-migrate-shared').dataType;

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
internals.migrationTable = 'migrations';

let db;

describe('db-migrate', () => {

    it('connect', () => {
        return new Promise((resolve, reject) => {
            driver.connect(config, internals, (err, _db) => {
                db = _db;
                if (err) {
                    return reject(err);
                }
                resolve();
            });
        });
    });

    it('createCollection', () => {
        return new Promise((resolve, reject) => {

            db.createCollection('event', () => {
                db._getCollectionNames((err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    expect(data.filter((collection) => collection.collectionName === 'event').length).to.equal(1);
                    db.dropCollection('event', () => {
                        resolve();
                    });
                });
            });

        });

    });

    it('dropCollection', () => {
        return new Promise((resolve, reject) => {

            db.createCollection('event', () => {
                db.dropCollection('event', () => {
                    db._getCollectionNames((err, data) => {
                        if (err) {
                            return reject(err);
                        }
                        expect(data.length).to.equal(0);
                        resolve();
                    });
                });
            });

        });

    });

    it('renameCollection', () => {
        return new Promise((resolve, reject) => {

            db.createCollection('event', () => {
                db.renameCollection('event', 'functions', () => {
                    db._getCollectionNames((err, data) => {
                        if (err) {
                            return reject(err);
                        }
                        expect(data.filter(collection => collection.collectionName === 'events').length).to.equal(0);
                        expect(data.filter(collection => collection.collectionName === 'functions').length).to.equal(1);
                        db.dropCollection('event', () => {
                            db.dropCollection('functions', () => {
                                resolve();
                            });
                        });
                    });
                });
            });
        });

    });

    it('addIndex', () => {
        return new Promise((resolve, reject) => {

            db.createCollection('event', () => {
                db.addIndex('event', 'event_title', 'title', false, () => {
                    db._getCollectionNames((err, collections) => {
                        if (err) {
                            return reject(err);
                        }
                        const collection = collections.filter(function (collection) {
                            return collection.collectionName === 'event';
                        });
                        let index = 0;
                        expect(collections).to.exist();
                        expect(collection.length).to.equal(1);

                        if (collections[0].collectionName === 'system.indexes') index = 1;

                        expect(collections[index].collectionName).to.equal('event');
                        db.dropCollection('event', () => {
                            resolve();
                        });
                    });
                });
            });
        });
    });

    it('insertOne', () => {
        return new Promise((resolve, reject) => {

            db.createCollection('event', () => {
                db.insert('event', { id: 2, title: 'title' }, (err) => {
                    if (err) {
                        return reject(err);
                    }

                    db._find('event', { title: 'title' }, (err, data) => {
                        if (err) {
                            return reject(err);
                        }
                        expect(data.length).to.equal(1);
                        db.dropCollection('event', () => {
                            resolve();
                        });
                    });
                }
                );
            });
        });
    });

    it('insertMany', () => {
        return new Promise((resolve, reject) => {
            db.createCollection('event', () => {
                db.insert('event',
                    [
                        { id: 2, title: 'title' },
                        { id: 3, title: 'lol' },
                        { id: 4, title: 'title' }
                    ],
                    (err) => {
                        if (err) {
                            return rejet(err);
                        }

                        db._find('event', { title: 'title' }, (err, data) => {
                            if (err) {
                                return reject(err);
                            }
                            expect(data.length).to.equal(2);
                            db.dropCollection('event', () => {
                                resolve();
                            });
                        });
                    });
            });
        });
    });

    it('removeIndex', () => {
        return new Promise((resolve, reject) => {

            db.createCollection('event', () => {
                db.addIndex('event', 'event_title', 'title', false, () => {
                    db.removeIndex('event', 'event_title', () => {
                        db._getIndexes('event', (err, indexes) => {
                            if (err) {
                                return reject(err);
                            }

                            expect(indexes).to.exist();
                            expect(indexes).to.not.include('event_title');
                            db.dropCollection('event', () => {
                                resolve();
                            });
                        });
                    });
                });
            });
        });
    });

    it('createMigrationsTable', () => {
        return new Promise((resolve, reject) => {
            db._createMigrationsCollection((err, data) => {
                if (err) {
                    return reject(err);
                }

                db._getCollectionNames((err, collections) => {
                    if (err) {
                        return reject(err);
                    }
                    let index = 0;
                    expect(collections).to.exist();
                    expect(collections.filter(function (collection) {
                        return collection.collectionName === 'migrations';
                    }).length).to.equal(1);
                    if (collections[0].collectionName === 'system.indexes') index = 1;

                    expect(collections[index].collectionName).to.equal('migrations');
                    db.dropCollection('migrations', () => {
                        resolve();
                    });
                });
            });
        });
    });

    it('updateMany', () => {
        return new Promise((resolve, reject) => {
            db.createCollection('event', (err) => {
                if (err) {
                    return reject(err);
                }
                db.insert('event', { id: 2, title: 'title' }, (err) => {
                    if (err) {
                        return reject(err);
                    }

                    const command = 'updateMany';
                    const renameFieldOptions = {
                        query: {},
                        update: {
                            $rename: {
                                title: 'titleUpdated'
                            }
                        },
                        options: {}
                    };

                    db._run(command, 'event', renameFieldOptions, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        db._find('event', { titleUpdated: 'title' }, (err, data) => {
                            expect(data.length).to.equal(1);
                            db.dropCollection('migrations', () => {
                                resolve();
                            });
                        });
                    });
                });
            });
        });
    });

    it('getDbInstance', () => {
        new Promise((resolve, reject) => {
            db.getDbInstance((err, dbInstance) => {
                if (err) {
                    return reject(err);
                }
                expect(dbInstance).to.exist();
                resolve();

            });
        });
    });
});