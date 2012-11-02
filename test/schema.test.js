"use strict";

var assert = require('assert');

var Schema = require('../lib/schema'),
    fields = require('../lib/fields'),
    StringField = fields.StringField,
    NumberField = fields.NumberField,
    JSONField = fields.JSONField,
    DateField = fields.DateField;

describe('Schema', function(){
    it('should construct fields just using the classnames', function(){
        var s = new Schema('Song', 'song', 'id', {
            'id': NumberField,
            'title': StringField
        });

        assert.equal(typeof s.fields.id, 'object');
        assert.equal(s.fields.id.name, 'id');
        assert.equal(typeof s.fields.title, 'object');
        assert.equal(s.fields.title.name, 'title');
    });

    it('should handle all field types', function(){
        var s = new Schema('Song', 'song', ['id', 'created'], {
            'id': NumberField,
            'title': StringField,
            'created': DateField,
            'recent_loves': JSONField
        });
        assert.equal(typeof s.fields.id, 'object');
        assert.equal(typeof s.fields.title, 'object');
        assert.equal(typeof s.fields.created, 'object');
        assert.equal(typeof s.fields.recent_loves, 'object');
    });

    it('should import raw data and marshall properly', function(){
        var row = {
                'id': '1',
                'title': 'Silence in a Sweater',
                'created': 1351373348257,
                'recent_loves': '[{"username": "lucas"}]'
            },
            created = new Date(row.created),
            s = new Schema('Song', 'song', 'id', {
                'id': NumberField,
                'title': StringField,
                'created': DateField,
                'recent_loves': JSONField
            }),
            data = s.import(row);

        assert.equal(data.id, 1);
        assert.equal(data.title, 'Silence in a Sweater');
        assert.equal(data.created.toString(), created.toString());
        assert.equal(data.recent_loves.length, 1);
    });

    it('should export data properly', function(){
        var row = {
                'id': '1',
                'title': 'Silence in a Sweater',
                'created': new Date(1351373348257),
                'recent_loves': [{"username": "lucas"}]
            },
            created = row.created,
            s = new Schema('Song', 'song', 'id', {
                'id': NumberField,
                'title': StringField,
                'created': DateField,
                'recent_loves': JSONField
            }),
            data = s.export(row);

        assert.equal(data.id.N, 1);
        assert.equal(data.title.S, 'Silence in a Sweater');
        assert.equal(data.created.N, created.getTime());
        assert.equal(data.recent_loves.S.length, JSON.stringify(row.recent_loves).length);
    });
});