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

    this.keySchema = {'HashKeyElement': {}};
    this.keySchema.HashKeyElement[this.hashType] = this.hash;


    this.schema = {
        'HashKeyElement': {
            'AttributeName': this.hash,
            'AttributeType': this.hashType
        }
    };

    if(this.range){
        this.keySchema.RangeKeyElement = {};
        this.keySchema.RangeKeyElement[this.rangeType] = this.range;
        this.schema.RangeKeyElement = {
            'AttributeName': this.range,
            'AttributeType': this.tangeType
        };
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
            row[key] = this.fields[key].import(row[key][this.fields[key].type]);
        }
        else{
            row[key] = this.fields[key].import();
        }
    }
    return row;
};

Schema.prototype.export = function(row){
    var data = {},
        key;

    Object.keys(this.fields).forEach(function(key){
        var field = this.field(key),
            value = row[key];

        if(field.isDefault(value)){
            // Don't sent empty sets as part of an update or put.
            // On import, this field value will be set to the default.
            return;
        }

        if(row.hasOwnProperty(key)){
            data[key] = {};
            data[key][field.type] = field.export(value);
        }
        else{
            data[key] = {};
            data[key][field.type] = field.export();
        }
    }.bind(this));
    return data;
};

Schema.prototype.exportKey = function(hash, range){
    var key = {'HashKeyElement': {}};
    if(hash === Object(hash) && !range){
        hash = hash[this.hash];
        range = hash[this.range];
    }
    key.HashKeyElement[this.hashType] = this.field(this.hash).export(hash);
    if(this.range){
        key.RangeKeyElement = {};
        key.RangeKeyElement[this.rangeType] = this.field(this.range).export(range);
    }
    return key;
};

module.exports = Schema;