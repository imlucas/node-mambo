"use strict";

var log = require('plog')('mambo.scan'),
    when = require('when'),
    util = require('util');

// this.scan('song', {
//         'available': {'EQ': '-1'},
//         'loves': {'NE': 0},
//         'title': 'NOT_NULL',
//         'artist': 'NULL',
//     },
//     {
//         'limit': 10,
//         'startKey': {
//             'id': 100
//         },
//         'fields': ['id', 'title']
//     });

function Scanner(model, alias){
    this.model = model;
    this.alias = alias;
    this.schema = this.model.schema(this.alias);

    this.filter = {};
    this._limit = undefined;
    this._fields = undefined;
    this._startKey = undefined;
    this._count = undefined;
    this._all = false;
}
module.exports = Scanner;

Scanner.prototype.all = function(){
    this._all = true;
    return this;
};

Scanner.prototype.count = function(){
    this._count = true;
    return this;
};

Scanner.prototype.limit = function(l){
    this._limit = l;
    return this;
};

Scanner.prototype.fields = function(f){
    this._fields = f;
    return this;
};

Scanner.prototype.start = function(key){
    this._startKey = key;
    return this;
};

// module.exports.scan('song')
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
Scanner.prototype.where = function(key, operator, value){
    operator = expandOperator(operator) || operator;
    var f = {};
    f[operator] = value;
    this.filter[key] = f;
    return this;
};

Scanner.prototype.fetch = function(){
    var opts = {},
        d = when.defer();

    ['limit', 'startKey', 'count', 'fields'].forEach(function(f){
        if(this['_' + f] !== undefined){
            opts[f] = this['_' + f];
        }
    }.bind(this));
    function getNextPage(res){
        var d = when.defer();
        if(res.hasMore){
            res.next().then(function(r){
                res.isLastPage = r.isLastPage;
                res.lastEvaluatedKey = r.lastEvaluatedKey;
                res.count += r.count;
                res.hasMore = r.hasMore;
                r.items.forEach(function(item){
                    res.items.push(item);
                });
                return d.resolve(getNextPage(res));
            }, d.reject);
        }
        else{
            log.debug('Got all pages.  Resolving');
            d.resolve(res);
        }
        return d.promise;
    }

    if(this._all){
        return this.model.runScan(this.alias, this.filter, opts).then(function(res){
            log.debug('Initial scan run.  Attempting to get next pages.');
            return getNextPage(res);
        }).then(function(res){
            return res;
        });
    }
    else{
        return this.model.runScan(this.alias, this.filter, opts);
    }
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

        case '=':
        case '==':
        case '===':
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

Scanner.Filter = Filter;

function ScanResult(model, alias, data, filter, opts){
    var self = this,
        hashField,
        rangeField;


    this.model = model;
    this.schema = this.model.schema(alias);
    this.alias = alias;
    this.filter = filter;
    this.opts = opts;

    this.count = data.Count;
    this.items = data.Items.map(function(item){
        return self.schema.import(item);
    });
    this.consumedCapacityUnits = data.ConsumedCapacityUnits;
    this.scannedCount = data.ScannedCount;
    this.isLastPage = data.LastEvaluatedKey === null || data.LastEvaluatedKey === undefined;
    this.hasMore = !this.isLastPage;
    this.lastEvaluatedKey = (this.isLastPage) ? null : {};
    if((!this.isLastPage) && (data.LastEvaluatedKey !== undefined)){
        hashField = this.schema.field(this.schema.hash);
        this.lastEvaluatedKey[hashField.name] = hashField.import(
            data.LastEvaluatedKey.HashKeyElement[hashField.shortType]
        );

        if(this.schema.range){
            rangeField = this.schema.field(this.schema.range);
            this.lastEvaluatedKey[rangeField.name] = rangeField.import(
                data.LastEvaluatedKey.RangeKeyElement[rangeField.shortType]
            );
        }
    }
}

// Get next page of results.
ScanResult.prototype.next = function(){
    log.debug('Getting next page for scan');
    var opts = this.opts;
    opts.startKey = this.lastEvaluatedKey;
    log.silly('Scan opts now: ' + util.inspect(opts));
    return this.model.runScan(this.alias, this.filter, opts);
};

Scanner.ScanResult = ScanResult;
