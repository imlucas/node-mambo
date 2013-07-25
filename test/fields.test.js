"use strict";

var assert = require('assert');

var Schema = require('../lib/schema'),
    fields = require('../lib/fields'),
    StringField = fields.StringField,
    StringSetField = fields.StringSetField,
    NumberField = fields.NumberField,
    NumberSetField = fields.NumberSetField,
    JSONField = fields.JSONField,
    DateField = fields.DateField,
    BooleanField = fields.BooleanField,
    CsvField = fields.CsvField,
    LongField = fields.LongField;

describe('Fields', function(){
    describe('String', function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new StringField('id').defaultValue, undefined);
            assert.deepEqual(new StringField('id', '').defaultValue, '');
        });
    });
    describe("StringSet", function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new StringSetField('tags').defaultValue, []);
        });
    });
    describe("Number", function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new NumberField('hits').defaultValue, 0);
        });
    });
    describe("NumberSet", function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new NumberSetField('hits').defaultValue, []);
        });
    });

    describe("JSON", function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new JSONField('followers').defaultValue, {});
            assert.deepEqual(new JSONField('followers', []).defaultValue, []);
        });
    });

    describe("Date", function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new DateField('created').defaultValue, 0);
            var d = new Date();
            assert.deepEqual(new DateField('created',
                function(){return d;}).defaultValue, d);
        });
    });

    describe("Boolean", function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new BooleanField('active').defaultValue, -1);
            var d = new Date();
            assert.deepEqual(new DateField('active', true).defaultValue, true);
        });
    });

    describe("Csv", function(){
        it('should have correct defaults', function(){
            assert.deepEqual(new CsvField('tags').defaultValue, undefined);
        });

        it('should cast an array to a string for storage', function(){
            assert.deepEqual(new CsvField('tags').export(['music', 'news']),
                'music,news');
        });

        it('should cast a string to a string for storage', function(){
            assert.deepEqual(new CsvField('tags').export('music,news'),
                'music,news');
        });

        it('should cast a string to an array for use', function(){
            assert.deepEqual(new CsvField('tags').import('music,news'),
                ['music', 'news']);
        });
    });

    describe("Long", function(){
        it('should have the right default', function(){
            assert.deepEqual(new LongField('id').defaultValue, '0');
        });

        it('should work with magic numbers', function(){
            assert.deepEqual(new LongField('id').export('15811513159681281'),
                '15811513159681281');
        });

        it('should work with magic numbers', function(){
            assert.deepEqual(new LongField('id').import('15811513159681281'),
                '15811513159681281');
        });
    });
});