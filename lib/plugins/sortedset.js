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

// songLoves.incrby(1, 1, function(){
//     songLoves.incrby(1, 1, function(){
//         songLoves.incrby(2, 3, function(){
//             songLoves.range(0, 2, function(err, ids){
//                 assert.deepEqual(ids, [2, 1]);
//                 songLoves.clear();
//             });
//         });
//     });
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
SortedSet.prototype.incrby = function(member, score, done){
    return this.model.update('set', this.key, member)
        .inc('score', score)
        .commit(done);
};

// Remove a member from the set.
SortedSet.prototype.remove = function(member, done){
    return this.model.delete(this.alias, this.key, member, done);
};

// Get the position of a member in a set.
// This will be slow and expensive for large sets so it should be cached.
SortedSet.prototype.rank = function(member, done){
    this.model.objects(this.alias, this.key).fetch(function(err, res){
        if(err){
            return done(err);
        }
        var members = res.map(function(row){
            return row.member;
        });
        done(null, members.indexOf(member));
    });
};

SortedSet.prototype.getRange = function(start, stop, direction, done){
    var query = this.model.objects(this.alias, this.key);
    if(direction !== 'asc'){
        query.reverse();
    }
    return query.index('score-index').fetch(done);
};

// Get a chunk of the set, ascending score.
SortedSet.prototype.revrange = function(start, stop, done){
    return this.getRange(start, stop, 'asc', function(err, res){
        if(err){
            return done(err);
        }

        done(null, res.map(function(item){
            return item.member;
        }));
    });
};

// Get a chunk of the set, descending score.
SortedSet.prototype.range = function(start, stop, done){
    this.getRange(start, stop, 'desc', function(err, res){
        if(err){
            return done(err);
        }
        done(null, res.map(function(item){
            return item.member;
        }));
    });
};

// Delete all members of the set.
SortedSet.prototype.clear = function(done){
    var self = this,
        batch = this.model.batch();

    this.model.objects(this.alias, this.key).fetch(function(err, res){
        if(err){
            return done(err);
        }
        if(res.length === 0){
            return done(null, []);
        }
        res.map(function(row){
            batch.remove(self.alias, row.key, row.member);
        });
        batch.commit(done);
    });
};

module.exports = SortedSet;