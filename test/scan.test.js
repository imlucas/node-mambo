"use strict";

var assert = require('assert');

var Scanner = require('../lib/scan'),
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


describe('Scan', function(){
    describe('API', function(){
        it('should return a query object when calling objects', function(){
            var s = Song.scan('loves', 1);
            assert.equal(s.constructor, Scanner);
        });


        it('should allow specifying a start key', function(){
            var n = Date.now(),
                s = Song.scan('loves').start({'id': 1, 'created': n});

            assert.deepEqual(s._startKey, {'id': 1, 'created': n});
        });

        it('should allow specifying a limit', function(){
            var q = Song.scan('loves').limit(5);
            assert.equal(q._limit, 5);
        });

        it('should allow specifying fields', function(){
            var s = Song.scan('loves').fields(['username', 'created']);
            assert.deepEqual(s._fields, ['username', 'created']);
        });

        it('should allow specifying a range key condition', function(){
            var s = Song.scan('loves').where('created', '>', 1);
            assert.deepEqual(s.filter, {'created': {'GT': 1}});
        });

    });
});