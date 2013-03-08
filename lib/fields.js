"use strict";
var util = require('util'),
    _ = require('underscore');

function valueOrCall(v){
    if(Object.prototype.toString.call(v) === '[object Function]'){
        return v.call();
    }
    return v;
}

function Field(name, type, defaultValue){
    defaultValue = valueOrCall(defaultValue);
    this.name = name;
    this.type = this.shortType = type;
    this.defaultValue = defaultValue !== undefined ? defaultValue : valueOrCall(this.defaultValue);
}
Field.prototype.type = 'Base';
Field.prototype.isSet = false;
Field.prototype.isDefault = function(v){
    return (v === null || v === undefined || _.isEqual(v, this.defaultValue));
};

// From dynamo
Field.prototype.import = function(value){
    throw new Error('Not implemented');
};

// To dynamo
Field.prototype.export = function(value){
    throw new Error('Not implemented');
};


function StringField(name, defaultValue){
    StringField.super_.call(this, name, 'S', defaultValue);
}
util.inherits(StringField, Field);

StringField.prototype.type = 'String';

StringField.prototype.import = function(value){
    if(value === 'null' || value === undefined){
        return null;
    }
    return value;
};

StringField.prototype.export = function(value){
    return (value && value.length > 0) ? value : "null";
};

function StringSetField(name, defaultValue){
    StringSetField.super_.call(this, name, 'SS', defaultValue);
}
util.inherits(StringSetField, Field);

StringSetField.prototype.defaultValue = [];

StringSetField.prototype.type = 'StringSet';

StringSetField.prototype.isSet = true;

StringSetField.prototype.import = function(value){
    if(value === undefined || value === null){
        return this.defaultValue;
    }
    return value;
};

StringSetField.prototype.export = function(value){
    if(value === undefined || value === null){
        return [];
    }
    return value.map(String);
};

function NumberField(name, defaultValue){
    NumberField.super_.call(this, name, 'N', defaultValue);
}
util.inherits(NumberField, Field);

NumberField.prototype.defaultValue = 0;

NumberField.prototype.type = 'Number';

NumberField.prototype.import = function(value){
    if(value === undefined || value === null){
        return this.defaultValue;
    }
    return Number(value);
};

NumberField.prototype.export = function(value){
    if(value === undefined){
        return "0";
    }
    return value.toString();
};

function NumberSetField(name, defaultValue){
    NumberSetField.super_.call(this, name, 'NS', defaultValue);
}
util.inherits(NumberSetField, Field);
NumberSetField.prototype.defaultValue = [];
NumberSetField.prototype.type = 'NumberSet';
NumberSetField.prototype.isSet = true;

NumberSetField.prototype.import = function(value){
    if(value === undefined){
        return this.defaultValue;
    }
    return value.map(Number);
};

NumberSetField.prototype.export = function(value){
    if(value === undefined){
        return this.defaultValue;
    }
    return value.map(String);
};

function JSONField(name, defaultValue){
    JSONField.super_.call(this, name, 'S', defaultValue);
}
util.inherits(JSONField, Field);
JSONField.prototype.defaultValue = {};
JSONField.prototype.type = 'JSON';

JSONField.prototype.import = function(value){
    if(value === undefined){
        return this.defaultValue;
    }
    return JSON.parse(value);
};

JSONField.prototype.export = function(value){
    if(value === undefined){
        value = null;
    }
    return JSON.stringify(value);
};

function DateField(name, defaultValue){
    DateField.super_.call(this, name, 'N', defaultValue);
}
util.inherits(DateField, Field);
DateField.prototype.defaultValue = 0;
DateField.prototype.type = 'Date';

DateField.prototype.import = function(value){
    if(!value || Number(value) === this.defaultValue){
        return null;
    }
    return new Date(Number(value));
};

DateField.prototype.export = function(value){
    if(value === undefined || value === null ||
            value === this.defaultValue){
        return this.defaultValue.toString();
    }
    if(value.constructor === Date){
        return value.getTime().toString();
    }

    return this.defaultValue;
};

function BooleanField(name, defaultValue){
    BooleanField.super_.call(this, name, 'N', defaultValue);
}
util.inherits(BooleanField, Field);
BooleanField.prototype.defaultValue = -1;
BooleanField.prototype.type = 'Boolean';

BooleanField.prototype.import = function(value){
    if(value === undefined || Number(value) === this.defaultValue){
        return null;
    }
    return Number(value) === 1 ? true : false;
};

BooleanField.prototype.export = function(value){
    if(value === undefined || value === null){
        value = this.defaultValue;
    }
    return String(Number(value));
};

module.exports.Field = Field;
module.exports.StringField = StringField;
module.exports.StringSetField = StringSetField;
module.exports.NumberField = NumberField;
module.exports.NumberSetField = NumberSetField;
module.exports.JSONField = JSONField;
module.exports.DateField = DateField;
module.exports.BooleanField = BooleanField;