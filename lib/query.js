"use strict";

// self.table('tableAlias')
//     .query('primaryKeyValue', 'optionalRangeValue')
//     .fetch()
//     .then(function(){
//     });

function Query(model, tableAlias, hash, range){
    this.model = model;
    this.tableAlias = tableAlias;
    this.hash = hash;
    this.range = range;
}

Query.prototype.fetch = function(){

};

module.exports = Query;