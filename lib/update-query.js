"use strict";
// self.table('tableAlias').update('primaryKeyValue', 'optionalRangeValue')
//     .returnNone()
//     .set({'someKey': 'someValue'})
//     .inc('lovesCount', 1)
//     .dec('followers')
//     .expect('attrName', 'attrValue')
//     .commit()
//     .then(function(data){
//         console.log('Data should be null because we specified fields as `null`');
//     });

function UpdateQuery(model, alias, hashValue){
    this.model = model;
    this.alias = alias;
    this.hash = hashValue;
    this.schema = this.model.schema(this.alias);

    this.range = null;
    this.expectations = [];
    this.ops = [];
    this.returnValues = 'UPDATED_NEW';
}

UpdateQuery.prototype.returnAllOld = function(){
    this.returnValues = 'ALL_OLD';
    return this;
};

UpdateQuery.prototype.returnAllNew = function(){
    this.returnValues = 'ALL_NEW';
    return this;
};

UpdateQuery.prototype.returnUpdatedOld = function(){
    this.returnValues = 'UPDATED_OLD';
    return this;
};

UpdateQuery.prototype.returnUpdatedNew = function(){
    this.returnValues = 'UPDATED_NEW';
    return this;
};

UpdateQuery.prototype.returnNone = function(){
    this.returnValues = 'NONE';
    return this;
};

// Set object properties
//    update('song', 123).set({
//        'artist': 'DJ JMarmz',
//        'album': 'Silence in a Sweater'
//    }).commit();
UpdateQuery.prototype.set = function(opts){
    for(var key in opts){
        if(Array.isArray(opts[key]) && opts[key].length === 0){
            this.ops.push({
                'action': 'DELETE',
                'attributeName': key
            });
        }
        else{
            this.ops.push({
                'action': 'PUT',
                'attributeName': key,
                'newValue': opts[key]
            });
        }

    }
    return this;
};

// Add to a set
//     update('song', 123).push('tags', ['electro', 'pop']).commit();
UpdateQuery.prototype.push = function(key, value){
    if(!Array.isArray(value)){
        value = [value];
    }
    this.ops.push({
        'action': 'ADD',
        'attributeName': key,
        'newValue': value
    });
    return this;
};

// Increment a numeric value
//     update('song', 123).inc('loves', 100000).inc('followers').commit();
UpdateQuery.prototype.inc = function(key, value){
    value = value || 1;
    this.ops.push({
        'action': 'ADD',
        'attributeName': key,
        'newValue': value
    });
    return this;
};

// Decrement a numeric value
//     update('song', 123).dec('loves', 5).dec('followers').commit();
UpdateQuery.prototype.dec = function(key, value){
    value = value || 1;
    return this.inc(key, value * -1);
};

// Set an expected value
UpdateQuery.prototype.expect = function(name, value, exists){
    var i = {
        'attributeName': name,
        'expectedValue': value
    };
    if(exists !== undefined){
        i.exists = exists;
    }
    this.expectations.push(i);
    return this;
};

// Commit the actual update to dynamo.
UpdateQuery.prototype.commit = function(){
    var extras = {};

    if(this.expectations.length > 0){
        extras.expectedValues = this.expectations;
    }

    if(this.returnValues){
        extras.returnValues = this.returnValues;
    }

    if(this.range){
        extras.range = this.range;
    }

    return this.model.updateItem(this.alias, this.hash, this.ops, extras);
};

module.exports = UpdateQuery;