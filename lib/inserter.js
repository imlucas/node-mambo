"use strict";

function Inserter(model, alias){
    this.model = model;
    this.alias = alias;

    this.expectations = [];
    this.ops = [];
    this.returnValues = 'ALL';
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


module.exports = Inserter;