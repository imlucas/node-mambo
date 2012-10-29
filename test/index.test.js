"use strict";

var assert = require('assert');

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

var Song = new mambo.Model(songSchema);

describe('Model', function(){
    it('should have all the schemas available after connecting', function(){
        Song.connect();
        assert.ok(Song.schema('song'), 'Should have song schema');
        assert.ok(Song.table('song'), 'Should have song table');
    });
});