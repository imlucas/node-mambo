"use strict";
// self.table('tableAlias').update('primaryKeyValue', 'optionalRangeValue')
//     .fields(null)
//     .set({'someKey': 'someValue'})
//     .inc('lovesCount', 1)
//     .dec('followers')
//     .expect('attrName', 'attrValue')
//     .commit()
//     .then(function(data){
//         console.log('Data should be null because we specified fields as `null`');
//     });

function UpdateQuery(model, tableAlias, hashValue){
    this.model = model;
    this.tableAlias = tableAlias;
    this.hash = hashValue;

    this.range = null;
    this.expectations = [];
    this.ops = [];
    this.returnValues = 'ALL';
}

// Set object properties
//    update('song', 123).set({
//        'artist': 'DJ JMarmz',
//        'album': 'Silence in a Sweater'
//    }).commit();
UpdateQuery.protoype.set = function(opts){
    for(var key in opts){
        this.ops.push({
            'action': 'PUT',
            'attributeName': key,
            'newValue': opts[key]
        });
    }
};

// Add to a set
//     update('song', 123).push('tags', ['electro', 'pop']).commit();
UpdateQuery.prototype.push = function(){
    throw new Error('Not implemented');
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
    this.ecxpectations.push(i);
    return this;
};

// Commit the actual update to dynamo.
UpdateQuery.prototype.commit = function(){
    var extras = {};

    if(this.expectations.length > 0){
        extras.expectedValues = this.expectations;
    }

    if(this.range){
        extras.range = this.range;
    }

    return this.model.updateItem(this.tableAlias, this.hash, this.ops, extras);
};
module.exports = UpdateQuery;