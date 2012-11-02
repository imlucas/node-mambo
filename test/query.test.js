"use strict";

var assert = require('assert');

var Query = require('../lib/query'),
    UpdateQuery = require('../lib/update-query'),
    mambo = require('../'),
    Schema = mambo.Schema,
    StringField = mambo.StringField,
    NumberField = mambo.NumberField,
    JSONField = mambo.JSONField,
    DateField = mambo.DateField,
    StringSetField = mambo.StringSetField,
    NumberSetField = mambo.NumberSetField;

var songSchema = new Schema('Song', 'song', 'id', {
    'id': NumberField,
    'title': StringField,
    'created': DateField,
    'recent_loves': JSONField,
    'tags': StringSetField,
    'no_ones': NumberSetField
});

var loveSchema = new Schema('SongLoves', 'loves', ['id', 'created'], {
    'id': NumberField,
    'username': StringField,
    'created': NumberField
});

var Song = new mambo.Model(songSchema, loveSchema);


describe('Query', function(){
    describe('API', function(){
        it('should return a query object when calling objects', function(){
            var q = Song.objects('loves', 1);
            assert.equal(q.constructor, Query);
        });

        it('should through a type error when trying to query on hash only schemas', function(){
            assert.throws(function(){
                Song.objects('song', 1);
            }, TypeError);
        });

        it('should allow specifying a start key', function(){
            var n = Date.now(),
                q = Song.objects('loves', 1).start(1, n);

            assert.deepEqual(q.startKey, [1, n]);
        });

        it('should allow specifying a limit', function(){
            var q = Song.objects('loves', 1).limit(5);
            assert.equal(q._limit, 5);
        });

        it('should allow reversing the returned data', function(){
            var q = Song.objects('loves', 1).reverse();
            assert.equal(q.scanForward, true);
        });

        it('should allow specifying fields', function(){
            var q = Song.objects('loves', 1).fields(['username', 'created']);
            assert.deepEqual(q.fieldNames, ['username', 'created']);
        });

        it('should return an update query instance when calling update()', function(){
            var q = Song.objects('loves', 1, Date.now()).update();
            assert.equal(q.constructor, UpdateQuery);
        });
    });
});