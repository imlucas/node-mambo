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

describe('Model', function(){
    beforeEach(function(done){
        magneto.server = magneto.listen(8081, function(s){
            Song.createAll().then(function(){
                done();
            });
        });
    });
    afterEach(function(){
        magneto.server.close();
    });
    it('should have all the schemas available after connecting', function(){
        Song.connect();

        assert.ok(Song.schema('song'), 'Should have song schema');
        assert.ok(Song.table('song'), 'Should have song table');
    });

    it('should handle basic puts', function(done){
        Song.insert('song').set({
            'id': 1,
            'title': 'Silence in a Sweater',
            'created': new Date(),
            'recent_loves': [{
                'username': 'lucas'
            }],
            'tags': [
                'silence',
                'sweaters'
            ],
            'no_ones': [
                Date.now()
            ]
        }).commit().then(function(){
            done();
        });
    });

    it('should handle batch inserts', function(done){
        Song.insert('song').set({
            'id': 1
        }).insert('song').set({
            'id': 2
        }).commit().then(function(){
            done();
        });
    });

    it('should automatically split batches that are too large', function(done){
        var q = Song.insert('song').set({
            'id': 1
        }), i = 2;
        for(i; i <= 50; i++){
            q.insert('song').set({
                'id': i
            });
        }

        q.commit().then(function(songs){
            assert.ok(Array.isArray(songs), 'Should give us back some response docs?');
            assert.equal(songs.length, 50);
            done();
        });
    });

    it('should handle batch inserts across tables', function(done){
        Song.insert('song').set({
            'id': 1
        }).insert('loves').set({
            'id': 1,
            'username': 'lucas',
            'created': new Date()
        }).commit().then(function(docs){
            assert.ok(docs.loves !== undefined, 'Should give us back some response docs?');
            assert.equal(docs.loves.length, 1);
            assert.equal(docs.songs.length, 1);
            done();
        });
    });
});