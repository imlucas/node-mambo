"use strict";
var log = require('plog')('mambo.search'),
    util = require('util'),
    Q = require('q'),
    Queue = require('./queue');

function Searchable(model){
    model.on('update', function(update){
        log.debug('New update on alias `'+update.alias +
            '` with hash `'+update.hash+'` ' + (update.range ? 'and range `' +
                update.range + '`' : ''));
        log.debug('Attribute updates: ' + util.inspect(update.updates));
        log.debug('Options: ' + util.inspect(update.options));
    });

    model.on('insert', function(insert){

    });
}

function Query(){}
Query.prototype.sort = function(key, direction){

};

Query.prototype.start = function(s){
    this.startAt = s;
    return this;
};

Query.prototype.limit = function(l){
    this.results = l;
    return this;
};

Query.prototype.fetch = function(){
    var d = Q.defer();
    return d.promise;
};


function SearchService(model){
    this.model = model;
    model.search = this.search;
    this.queues = [];
}

SearchService.prototype.domains = [];
SearchService.prototype.domain = function(d){
    var q = new Queue(d.name + '-cs-updates'),
        self = this;

    this.model.on('update', function(update){
        if(update.alias === self.alias){
            update.op = 'ADD';
            update.version = Date.now();
            q.put(update);
        }
    });

    this.model.on('insert', function(insert){
        if(insert.alias === this.alias){
            insert.op = 'ADD';
            insert.version = Date.now();
            q.put(insert);
        }
    }.bind(this));

    this.model.on('delete', function(del){
        if(del.alias === this.alias){
            del.op = 'DELETE';
            del.version = Date.now();
            q.put(del);
        }
    }.bind(this));

    setTimeout(function(){

    }, 1000 * 60 * 15);

    this.domains.push(d);
    return this;
};

SearchService.prototype.search = function(q){
    return new Query(this, q);
};

SearchService.prototype.createAll = function(){
};

function Domain(modelAlias, schema){
    var self = this;
    Object.keys(schema).map(function(key){
        var Field = schema[key];
        if(typeof field === 'function'){
            Field = new Field({});
        }
        Field.name = key;
        self.fields[Field.name] = Field;
    });
}

Domain.prototype.domainName = undefined;
Domain.prototype.idField = undefined;
Domain.prototype.allows = [];
Domain.prototype.indexFields = {};
Domain.prototype.rankExpressions = {};
Domain.prototype.useAutoType = true;

Domain.prototype.name = function(n){
    this.domainName = n;
    return this;
};

Domain.prototype.id = function(key){
    this.idField = key;
    return this;
};

Domain.prototype.allow = function(ip){
    this.allows.push(ip);
    return this;
};

Domain.prototype.rank = function(name, expr){
    this.rankExpressions[name] = expr;
};

Domain.prototype.getSDF = function(model){
    // Apply defaults and shit.
};

function valueOrCall(v){
    if(Object.prototype.toString.call(v) === '[object Function]'){
        return v.call();
    }
    return v;
}

function StringIndex(opts){
    this.mapsTo = opts.to;
    this.name = opts.name;

    this.defaultValue = opts.default !== undefined ? valueOrCall(opts.default) :
        valueOrCall(this.defaultValue);
}

function NumberIndex(opts){
    this.mapsTo = opts.to;
    this.name = opts.name;

    this.defaultValue = opts.default !== undefined ? valueOrCall(opts.default) :
        valueOrCall(this.defaultValue);
}

function DateIndex(opts){
    this.mapsTo = opts.to;
    this.name = opts.name;

    this.defaultValue = opts.default !== undefined ? valueOrCall(opts.default) :
        valueOrCall(this.defaultValue);
}
function Searchable(){}

var feedDomain = {
        'alias': 'feed',
        'schema': {
            'url': StringIndex,
            'statusCode': new NumberIndex({'default': -1, 'to': 'statusCode'}),
            'lastChanged': DateIndex,
            'lastFetched': DateIndex,
            'nextFetch': DateIndex,
            'type': StringIndex
        },
        'ranks': {
            'recentlyFetched': 'lastFetched'
        }
    };

var cs = new S(model)
    .allow('0.0.0.0/0')
    .name('freddy')
    .add({
        'alias': 'feed',
        'schema': {
            'url': StringIndex,
            'statusCode': new NumberIndex({'default': -1, 'to': 'statusCode'}),
            'lastChanged': DateIndex,
            'lastFetched': DateIndex,
            'nextFetch': DateIndex,
            'type': StringIndex
        },
        'ranks': {
            'recentlyFetched': 'lastFetched'
        }
    })
    .create();

model.search('feed', {'url': 'tumblr'})
    .sort('lastChanged', 'desc')
    .start(0)
    .limit(20)
    .fetch()
    .then(function(results){

    });