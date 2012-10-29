"use strict";

function Inserter(model, alias){
    this.model = model;
    this.alias = alias;

    this.expectations = [];
    this.ops = [];
}

Inserter.prototype.set = function(opts){
    for(var key in opts){
        this.ops.push({
            'action': 'PUT',
            'attributeName': key,
            'newValue': opts[key]
        });
    }
};

Inserter.prototype.commit = function(){

};

Inserter.prototype.insert = function(alias){
    // Upgrade to batch inserter
    var i = new BatchInserter(this.model);
    i.fromInserter(this);
    return i;
};

function BatchInserter(model){
    this.model = model;
}

BatchInserter.prototype.insert = function(alias){

};

BatchInserter.prototype.fromInserter = function(inserter){

};

BatchInserter.prototype.set = function(opts){

};

BatchInserter.prototype.commit = function(){

};

module.exports = Inserter;