"use strict";
var util = require('util');

function Field(name, type){
    this.name = name;
    this.type = type;
}

// From dynamo
Field.prototype.import = function(value){
    throw new Error('Not implemented');
};

// To dynamo
Field.prototype.export = function(value){
    throw new Error('Not implemented');
};

Field.prototype.toString = function(){
    return "Field('"+this.name+"')";
};

function StringField(name){
    StringField.super_.call(this, name, 'S');
}
util.inherits(StringField, Field);

StringField.prototype.import = function(value){
    return value;
};

StringField.prototype.export = function(value){
    return (value && value.length > 0) ? value : null;
};

StringField.prototype.toString = function(){
    return "StringField('"+this.name+"')";
};

function StringSetField(name){
    StringSetField.super_.call(this, name, 'SS');
}
util.inherits(StringSetField, Field);

StringSetField.prototype.import = function(value){
    return value;
};

StringSetField.prototype.export = function(value){
    return value;
};

StringSetField.prototype.toString = function(){
    return "StringSetField('"+this.name+"')";
};

function NumberField(name){
    NumberField.super_.call(this, name, 'N');
}
util.inherits(NumberField, Field);

NumberField.prototype.import = function(value){
    return Number(value);
};

NumberField.prototype.export = function(value){
    return value.toString();
};

NumberField.prototype.toString = function(){
    return "NumberField('"+this.name+"')";
};

function NumberSetField(name){
    NumberSetField.super_.call(this, name, 'NS');
}
util.inherits(NumberSetField, Field);

NumberSetField.prototype.import = function(value){
    return value;
};

NumberSetField.prototype.export = function(value){
    return value;
};

NumberSetField.prototype.toString = function(){
    return "NumberSetField('"+this.name+"')";
};

function JSONField(name){
    JSONField.super_.call(this, name, 'S');
}
util.inherits(JSONField, Field);

JSONField.prototype.import = function(value){
    return JSON.parse(value);
};

JSONField.prototype.export = function(value){
    return JSON.stringify(value);
};

JSONField.prototype.toString = function(){
    return "JSONField('"+this.name+"')";
};

function DateField(name){
    DateField.super_.call(this, name, 'N');
}
util.inherits(DateField, Field);

DateField.prototype.import = function(value){
    if(!value){
        throw new Error(this.name + ' is not a Date object! ' + value);
    }
    return new Date(value);
};

DateField.prototype.export = function(value){
    try{
        return value.getTime();
    }
    catch(e){
        throw new Error(this.name + ' is not a Date object! ' + value);
    }
};

DateField.prototype.toString = function(){
    return "DateField('"+this.name+"')";
};

function BooleanField(name){
    BooleanField.super_.call(this, name, 'N');
}
util.inherits(BooleanField, Field);

BooleanField.prototype.import = function(value){
    return Number(value) === 1 ? true : false;
};

BooleanField.prototype.export = function(value){
    return Number(value) === 1 ? true : false;
};

BooleanField.prototype.toString = function(){
    return "BooleanField('"+this.name+"')";
};



module.exports.Field = Field;
module.exports.StringField = StringField;
module.exports.StringSetField = StringSetField;
module.exports.NumberField = NumberField;
module.exports.NumberSetField = NumberSetField;
module.exports.JSONField = JSONField;
module.exports.DateField = DateField;
module.exports.BooleanField = BooleanField;