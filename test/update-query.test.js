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


describe('UpdateQuery', function(){
    describe('API', function(){
        it('should not have range if none specified', function(){
            var q = Song.update('song', 1);
            assert.equal(q.range, null);
        });

        it('should allow simple puts', function(){
            var q = Song.update('song', 1).set({
                'title': 'Silence in a Sweatervest'
            });
            assert.deepEqual(q.ops, [
                {
                    'action': 'PUT',
                    'attributeName': 'title',
                    'newValue': 'Silence in a Sweatervest'
                }
            ]);
        });

        it('should allow inc\'s on numeric fields', function(){
            var q = Song.update('song', 1).inc('loves');
            assert.deepEqual(q.ops, [
                {
                    'action': 'ADD',
                    'attributeName': 'loves',
                    'newValue': 1
                }
            ]);
        });

        it('should allow decrements on numeric fields', function(){
            var q = Song.update('song', 1).dec('loves');
            assert.deepEqual(q.ops, [
                {
                    'action': 'ADD',
                    'attributeName': 'loves',
                    'newValue': -1
                }
            ]);
        });

        it('should allow specifiying expected conditionals easily', function(){
            var q = Song.update('song', 1).expect('loves', 1, true);
            assert.deepEqual(q.expectations, [
                {
                    'attributeName': 'loves',
                    'expectedValue': 1,
                    'exists': true
                }
            ]);
        });

        it('should allow pushing a value onto a set', function(){
            var q = Song.update('song', 1).push('tags', 'chillwave');
            assert.deepEqual(q.ops, [
                {
                    'action': 'ADD',
                    'attributeName': 'tags',
                    'newValue': 'chillwave'
                }
            ]);
        });

        it('should allow pushing multiple values onto a set', function(){
            var q = Song.update('song', 1).push('tags', ['chillwave', 'dance']);
            assert.deepEqual(q.ops, [
                {
                    'action': 'ADD',
                    'attributeName': 'tags',
                    'newValue': 'chillwave'
                },
                {
                    'action': 'ADD',
                    'attributeName': 'tags',
                    'newValue': 'dance'
                }
            ]);
        });
    });
});