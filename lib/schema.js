"use strict";
function Schema(alias, keys, decl){
    this.alias = alias;
    this.tableName = alias;
    this.hash = undefined;
    this.range = undefined;

    if(Array.isArray(keys)){
        this.hash = keys[0];
        this.range = (keys.length) > 1 ? keys[1] : undefined;
    }
    else{
        this.hash = keys;
    }
    this.fields = {};
    for(var key in decl){
        var field = decl[key];
        if(typeof field === 'function'){
            field = new field(key);
        }
        this.fields[field.name] = field;
    }
}

Schema.prototype.import = function(row){
    for(var key in this.fields){
        if(row.hasOwnProperty(key)){
            row[key] = this.fields[key].import(row[key]);
        }
    }
    return row;
};


Schema.prototype.export = function(row){
    for(var key in this.fields){
        if(row.hasOwnProperty(key)){
            row[key] = this.fields[key].export(row[key]);
        }
    }
    return row;
};

module.exports = Schema;