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
    this.filter = {};
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

// module.exports.query('song')
//     // .where('id', 'IN', [1, 2, 3])
//     // .where('loves', '<', 1)
//     // .where('trending', '=', null)
//     // .where('title', '!=', null)
//     // .where('bio', 'NULL')
//     // .where('location', 'NOT_NULL')
//     // .where('started', 'BETWEEN', [1990, 1992])
//     // .fetch()
//     // .then(function(results){
//     // });
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
    return ret;
};

module.exports = Query;