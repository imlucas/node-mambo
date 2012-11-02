"use strict";
function Schema(tableName, alias, keys, decl){
    this.alias = alias;
    this.tableName = tableName;
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

    this.hashType = this.fields[this.hash].type;
    if(this.range){
        this.rangeType = this.fields[this.range].type;
    }


    this.schema = {};
    this.schema[this.hash] = this.primitive(this.hashType);
    if(this.range){
        this.schema[this.range] = this.primitive(this.rangeType);
    }
}

Schema.prototype.primitive = function(type){
    return (type==='S') ? String : Number;
};

Schema.prototype.field = function(key){
    return this.fields[key];
};

Schema.prototype.import = function(row){
    for(var key in this.fields){
        if(row.hasOwnProperty(key)){
            row[key] = this.fields[key].import(row[key]);
        }
    }
    return row;
};


Schema.prototype.export = function(row){
    var data = {},
        key;

    Object.keys(this.fields).forEach(function(key){
        if(row.hasOwnProperty(key)){
            var field = this.field(key);
            data[key] = {};
            data[key][field.type] = field.export(row[key]);
        }
    }.bind(this));
    return data;
};

Schema.exportKey = function(row){
    var key = {'HashKeyElement': {}};
    key.HashElementKey[this.field(this.hash).type] = this.field(this.hash).export(
        row[this.hash]);
    if(this.range){
        key.RangeKeyElement = {};
        key.RangeKeyElement[this.field(this.range).type] = this.field(this.range).export(
            row[this.range]);
    }
    return key;
};

module.exports = Schema;