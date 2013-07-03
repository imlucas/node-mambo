"use strict";

// self.table('tableAlias')
//     .query('primaryKeyValue', 'optionalRangeValue')
//     .fetch(done);
//
// this.query('edit', id)
//     .fetch(done);

// this.query('edit', id)
//     .remove(done);

// this.put('edit', id, range)
//     .set({})

// this.query('edit', id)
//     .update()
//     .set()
//     .commit(done);

// this.batch()
//     .add('edit');

var UpdateQuery = require('./update-query'),
    debug = require('plog')('mambo:query');

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
    this._count = false;
    this.scanForward = true;
    this.startKey = null;
    this.filter = {};
    this.useIndex = null;
    this._offset = 0;
}

// @todo (lucas) Add where to implement range key conditions.
Query.prototype.start = function(hash, range){
    this.startKey = [hash, range];
    return this;
};

Query.prototype.count = function(){
    this._count = true;
    return this;
};

Query.prototype.limit = function(l){
    this._limit = l + this._offset;
    return this;
};

Query.prototype.offset = function(o){
    this._offset = o;
    return this;
};

Query.prototype.consistent = function(){
    this.consistentRead = true;
    return this;
};

Query.prototype.reverse = function(){
    this.scanForward = false;
    return this;
};

Query.prototype.fields = function(fieldNames){
    this.fieldNames = fieldNames;
    return this;
};

Query.prototype.update = function(){
    return new UpdateQuery(this.model, this.alias, this.hash, this.range);
};

// Use a secondary index.
Query.prototype.index = function(name){
    this.useIndex = name;
    return this;
};

Query.prototype.fetch = function(done){
    var opts = {},
        self = this,
        schema = this.model.schema(this.alias);

    if(this._limit > -1){
        this._limit += this._offset;
        opts.limit = this._limit;
    }

    if(this.consistentRead){
        opts.consistentRead = this.consistentRead;
    }

    if(!this.scanForward){
        opts.scanIndexForward = this.scanForward;
    }

    if(this.fieldNames){
        opts.attributesToGet =  this.fieldNames;
    }

    if(this.useIndex){
        opts.index = this.useIndex;
    }

    if(this._count){
        opts.count = true;
    }

    if(this.filter){
        Object.keys(this.filter).forEach(function(key){
            var f = new Filter(schema, key, this.filter[key]);
            opts.rangeKeyCondition = f.export();
        }.bind(this));
    }
    debug('Calling query for `' + this.alias + '`');

    this.model.query(this.alias, this.hash, opts, function(err, items, count){
        if(err) return done(err);

        // If there is a user supplied offset, pre-slice down the items
        // to what the user actually wants.
        if(self._offset > 0){
            items = items.slice(self._offset, self._limit);
        }
        done(null, items, count);
    });
};

// module.exports.query('song')
//     // .where('id', 'IN', [1, 2, 3])
//     // .where('loves', '<', 1)
//     // .where('trending', '=', null)
//     // .where('title', '!=', null)
//     // .where('bio', 'NULL')
//     // .where('location', 'NOT_NULL')
//     // .where('started', 'BETWEEN', [1990, 1992])
//     // .fetch(done);
Query.prototype.where = function(key, operator, value){
    operator = expandOperator(operator) || operator;
    var f = {};
    f[operator] = value;
    this.filter[key] = f;
    return this;
};

function expandOperator(op){
    switch(op){
    case '>':
        return 'GT';

    case '>=':
        return 'GE';

    case '<':
        return 'LT';

    case '<=':
        return 'LE';

    case '!=':
        return 'NE';

    case '=' || '==' || '===':
        return 'EQ';

    default:
        return undefined;
    }
}


var opToArgLength = {
    'EQ': 1,
    'NE': 1,
    'LE': 1,
    'LT': 1,
    'GE': 1,
    'GT': 1,
    'NOT_NULL': 0,
    'NULL': 0,
    'CONTAINS': 1,
    'NOT_CONTAINS': 1,
    'BEGINS_WITH': 1,
    'IN': null,
    'BETWEEN': 2
};

function Filter(schema, key, data){
    this.schema = schema;
    this.field = schema.field(key);
    this.operator = undefined;
    this.value = [];

    if(data === new Object(data)){
        this.operator = Object.keys(data)[0];
        this.value = data[this.operator];
        if(!Array.isArray(this.value)){
            this.value = [this.value];
        }
    }
    else {
        this.operator = data;
        // @todo (lucas) Assert its actually a valueles condition.
    }
    var expectLen = opToArgLength[this.operator];
    if(expectLen !== null && this.value.length !== expectLen){
        throw new Error('Invalid number of args for ' +
            key + ' filter: ' +this.operator + ' ' + this.value);
    }
}

Filter.prototype.export = function(){
    var ret = {
        'AttributeValueList': [],
        'ComparisonOperator': 'EQ'
    };
    if(this.value){
        ret.AttributeValueList = this.value.map(function(val){
            var ret = {};
            ret[this.field.shortType] = this.field.export(val);
            return ret;
        }.bind(this));
    }
    if(this.operator){
        ret.ComparisonOperator = this.operator;
    }

    return ret;
};

module.exports = Query;