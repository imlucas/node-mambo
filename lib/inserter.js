"use strict";

var when = require('when');

function Inserter(model, alias){
    this.model = model;
    this.alias = alias;

    this.expectations = {};
    this.ops = {};
}

Inserter.prototype.set = function(opts){
    for(var key in opts){
        this.ops[key] = opts[key];
    }
    return this;
};

Inserter.prototype.commit = function(){
    var d = when.defer();
    this.model.put(this.alias, this.ops,
        this.expectations, true).then(
        function(data){
            d.resolve(data);
        },
        function(err){
            d.reject(err);
        }
    );
    return d.promise;
};

Inserter.prototype.insert = function(alias){
    // Upgrade to batch inserter
    var i = new BatchInserter(this.model);
    i.fromInserter(this);
    return i;
};

function BatchInserter(model){
    this.model = model;
    this.lastAlias = null;
    this.puts = {};
}

BatchInserter.prototype.insert = function(alias){
    this.lastAlias = alias;
    if(!this.puts.hasOwnProperty(alias)){
        this.puts[alias] = [];
    }
    return this;
};

BatchInserter.prototype.fromInserter = function(inserter){
    this.insert(inserter.alias);
    this.set(inserter.ops);
    return this;
};

BatchInserter.prototype.set = function(ops){
    this.puts[this.lastAlias].push(ops);
    return this;
};

BatchInserter.prototype.commit = function(){
    var d = when.defer();
    this.model.batchWrite(this.puts, {}).then(d.resolve, d.reject);
    return d.promise;
};

module.exports = Inserter;