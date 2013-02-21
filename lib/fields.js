"use strict";
var util = require('util'),
    _ = require('underscore'),
    BloomFilter = require('./bloomy');

function Field(name, type){
    this.name = name;
    this.type = this.shortType = type;
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


function StringField(name){
    StringField.super_.call(this, name, 'S');
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

function StringSetField(name){
    StringSetField.super_.call(this, name, 'SS');
    this.defaultValue = [];
}
util.inherits(StringSetField, Field);

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

function NumberField(name){
    NumberField.super_.call(this, name, 'N');
    this.defaultValue = 0;
}
util.inherits(NumberField, Field);

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

function NumberSetField(name){
    NumberSetField.super_.call(this, name, 'NS');
    this.defaultValue = [];
}
util.inherits(NumberSetField, Field);

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

function JSONField(name){
    JSONField.super_.call(this, name, 'S');
    this.defaultValue = null;
}
util.inherits(JSONField, Field);

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

function DateField(name){
    DateField.super_.call(this, name, 'N');
    this.defaultValue = 0;
}
util.inherits(DateField, Field);

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

function BooleanField(name){
    BooleanField.super_.call(this, name, 'N');
    this.defaultValue = -1;
}
util.inherits(BooleanField, Field);

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

function BinaryField(name){
    BinaryField.super_.call(this, name, 'B');
    this.defaultValue = new Buffer().toString('binary');
}
util.inherits(BinaryField, Field);

BinaryField.prototype.type = 'Binary';

BinaryField.prototype.import = function(value){
    if(value === undefined || Number(value) === this.defaultValue){
        return null;
    }
    return new Buffer(value, 'base64').toString('binary');
};

BinaryField.prototype.export = function(value){
    if(value === undefined || value === null){
        value = this.defaultValue;
    }
    return new Buffer(value, 'binary').toString('base64');
};


function BloomFilterField(name, errorRate){
    this.errorRate = errorRate || 0.1;
    BloomFilterField.super_.call(this, name, 'B');
    this.defaultValue = null;
}
util.inherits(BloomFilterField, Field);
BloomFilterField.prototype.type = 'BloomFilter';

BloomFilterField.prototype.import = function(value){
    if(value === undefined){
        return new BloomFilter(this.errorRate);
    }
    else{
        // console.log('importing ', value, new Buffer(value, 'base64'));
    }
    return BloomFilter.unserialize(new Buffer(value, 'base64'));
};

BloomFilterField.prototype.export = function(value){
    if(value === undefined || value === null){
        value = new BloomFilter(this.errorRate);
    }
    else{
        console.log('exporting', util.inspect(value, false, 10));
        console.log(value.has(new Buffer("lucas")));

    }
    return value.serialize().toString('base64');
};

module.exports.Field = Field;
module.exports.StringField = StringField;
module.exports.StringSetField = StringSetField;
module.exports.NumberField = NumberField;
module.exports.NumberSetField = NumberSetField;
module.exports.JSONField = JSONField;
module.exports.DateField = DateField;
module.exports.BooleanField = BooleanField;
module.exports.BinaryField = BinaryField;
module.exports.BloomFilterField = BloomFilterField;
