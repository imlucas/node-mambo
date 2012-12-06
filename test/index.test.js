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

magneto.setLogLevel(50);

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
            Song.get('song', 1).then(function(s){
                done();
            });
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
            q = q.insert('song').set({
                'id': i
            });
        }
        assert.equal(q.numOps, 50);
        assert.equal(q.puts.song.length, 50);

        q.commit().then(function(songs){
            done();
        });
    });

    it('should handle batch inserts across tables', function(done){
       var q = Song.insert('song').set({
            'id': 1
        }).insert('loves').set({
            'id': 1,
            'username': 'lucas',
            'created': new Date()
        });

        assert.equal(q.lastAlias, 'loves');
        q.commit().then(function(res){
            assert.equal(res.success.loves, 1);
            assert.equal(res.success.song, 1);
            Song.objects('loves', 1).fetch().then(function(loves){
                done();
            });
        });
    });
});