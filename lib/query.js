"use strict";

// self.table('tableAlias')
//     .query('primaryKeyValue', 'optionalRangeValue')
//     .fetch()
//     .then(function(){
//     });
// this.query('edit', id)
//     .fetch()
//     .then(d.resolve);

// this.query('edit', id)
//     .remove().then(d.resolve);

// this.put('edit', id, range)
//     .set({})
//     .then(d.resolve);

// this.query('edit', id)
//     .update()
//     .set()
//     .commit().then(d.resolve);

// this.batch()
//     .add('edit');

var UpdateQuery = require('./update-query');

function Query(model, alias, hash, range){
    if(!model.schema(alias).range){
        throw new TypeError('Query only for hash/range tables.  Should call Model.get.');
    }
    this.model = model;
    this.alias = alias;
    this.hash = hash;
    this.range = range;
    this.fieldNames = null;
    this.consistentRead = false;
    this._limit = -1;
    this.scanForward = false;
    this.startKey = null;
}

// @todo (lucas) Add where to implement range key conditions.
Query.prototype.start = function(hash, range){
    this.startKey = [hash, range];
    return this;
};

Query.prototype.limit = function(l){
    this._limit = l;
    return this;
};

Query.prototype.consistent = function(){
    this.consistentRead = true;
    return this;
};

Query.prototype.reverse = function(){
    this.scanForward = true;
    return this;
};

Query.prototype.fields = function(fieldNames){
    this.fieldNames = fieldNames;
    return this;
};

Query.prototype.update = function(){
    return new UpdateQuery(this.model, this.alias, this.hash, this.range);
};

Query.prototype.fetch = function(){
    var opts = {};
    if(this._limit > -1){
        opts.limit = this._limit;
    }

    if(this.consistentRead){
        opts.consistentRead = this.consistentRead;
    }

    if(this.scanForward){
        opts.scanIndexForward = this.scanForward;
    }

    if(this.fieldNames){
        opts.attributesToGet =  this.fieldNames;
    }
    return this.model.query(this.alias, this.hash, opts);
};

module.exports = Query;