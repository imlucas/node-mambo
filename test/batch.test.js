"use strict";

var assert = require('assert'),
    magneto = require('magneto');

magneto.server = null;

var mambo = require('../'),
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


describe('Batch', function(){
    it("should allow simple inline inserts", function(){
        var q = Song.batch()
            .insert('song', {
                'id': 1
            })
            .insert('song', {
                'id': 1
            });
        assert.equal(q.numOps, 2);
    });

    it("should allow set's on inserts", function(){
        var q = Song.batch()
            .insert('song')
            .set({
                'id': 1
            });
        assert.equal(q.numOps, 1);
    });

    it("should automatically recognize background queries", function(){
        var q = Song.batch()
            .remove('loves', 1);
        assert.equal(q.numOps, 0);
        assert.deepEqual(q.requiredScans, [
            {
                'alias': 'loves',
                'hash': 1
            }
        ]);
    });

    it("should skip background queries if not needed", function(){
        var q = Song.batch()
            .remove('loves', 1, new Date());
        assert.equal(q.numOps, 1);
        assert.deepEqual(q.requiredScans, []);
    });

    it("should throw trying to add an update to a batch", function(){
        assert.throws(function(){
            Song.batch().update('song', 1);
        }, Error);
    });

    it("should handle gets", function(){
        var batch = Song.batch().get('song', 1);
        assert.deepEqual(batch.gets, {
            'song': {
                'alias': 'song',
                'hashes': [1]
            }
        });
    });


});