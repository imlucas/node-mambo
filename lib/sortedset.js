"use strict";

var Schema = require('./schema'),
    Fields = require('./fields'),
    StringField = Fields.StringField,
    NumberField = Fields.NumberField,
    IndexField = Fields.IndexField,
    Model = require('../');

// var assert = require('assert'),
//     d = new Date(),
//     songLoves = new SortedSet('loves-' + [d.getMonth(), d.getDay(), d.getYear()].join('-'));
// songLoves.clear().then(function(){
//     return songLoves.incrby(1, 1);
// }).then(function(){
//     return songLoves.incrby(1, 1);
// }).then(function(){
//     return songLoves.incrby(2, 3);
// }).then(function(){
//     return songLoves.range(0, 2);
// }).then(function(ids){
//     assert.deepEqual(ids, [2, 1]);
//     return songLoves.clear();
// });
function SortedSet(key, table){
    table = table || 'SortedSet-' + key;
    this.alias = 'set-' + key;

    this.schema = new Schema(table, this.alias, ['key', 'member'], {
        'key': StringField,
        'member': StringField,
        'score': NumberField,
        'score-index': new IndexField('score').project(['member'])
    });
    this.key = key;
    this.model = new Model(this.schema);
}

// Increment the score of a member by {score}.
// If the member is not already in the set, it will be added and the score
// will be initialized with the value of {score}.
SortedSet.prototype.incrby = function(member, score){
    return this.model.update('set', this.key, member)
        .inc('score', score)
        .commit();
};

// Remove a member from the set.
SortedSet.prototype.remove = function(member){
    return this.model.delete(this.alias, this.key, member);
};

// Get the position of a member in a set.
// This will be slow and expensive for large sets so it should be cached.
SortedSet.prototype.rank = function(member){
    return this.model.query(this.alias, this.key).fetch().then(function(res){
        var members = res.map(function(row){
            return row.member;
        });
        return members.indexOf(member);
    });
};

SortedSet.prototype.getRange = function(start, stop, direction){
    return this.model.query(this.alias, undefined, {
        'conditions': {
            'key': {
                'EQ': this.key
            }
        },
        'scanIndexForward': direction === 'asc',
        'index': this.schema.index('score-index').name
    });
};

// Get a chunk of the set, ascending score.
SortedSet.prototype.revrange = function(start, stop){
    return this.getRange(start, stop, 'asc').then(function(res){
        return res.map(function(item){
            return item.member;
        });
    });
};

// Get a chunk of the set, descending score.
SortedSet.prototype.range = function(start, stop){
    return this.getRange(start, stop, 'desc').then(function(res){
        return res.map(function(item){
            return item.member;
        });
    });
};

// Delete all members of the set.
SortedSet.prototype.clear = function(){
    var self = this,
        batch = this.model.batch();

    return this.model.query(this.alias, this.key).fetch().then(function(res){
        if(res.length === 0){
            return [];
        }
        res.map(function(row){
            batch.remove(self.alias, row.key, row.member);
        });
        batch.commit().then(function(){
            return res;
        });
    });
};

module.exports = SortedSet;