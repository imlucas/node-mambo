"use strict";

var aws = require("plata"),
    when = require("when"),
    sequence = require("sequence"),
    _ = require("underscore"),
    Query = require('./lib/query'),
    UpdateQuery = require('./lib/update-query'),
    Batch = require('./lib/batch'),
    Schema = require('./lib/schema'),
    fields = require('./lib/fields'),
    Inserter = require('./lib/inserter'),
    Scanner = require('./lib/scan'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter,
    plog = require('plog'),
    log = plog('mambo').level('silly');

var instances = [];

// Models have many tables.
function Model(){
    this.connected = false;
    this.db = null;

    this.schemas = Array.prototype.slice.call(arguments, 0);
    this.schemasByAlias = {};

    this.schemas.forEach(function(schema){
        this.schemasByAlias[schema.alias] = schema;
    }.bind(this));

    this.tablesByName = {};
    instances.push(this);
}
util.inherits(Model, EventEmitter);

// Grab a schema definition by alias.
Model.prototype.schema = function(alias){
    return this.schemasByAlias[alias];
};

Model.prototype.tableNameToAlias = function(name){
    return this.tablesByName[name].alias;
};

// Fetch a query wrapper Django style.
Model.prototype.objects = function(alias, hash, range){
    if(typeof range === 'object'){
        var d = when.defer(),
            key = Object.keys(range)[0],
            q = new Query(this, alias, hash);
        q.fetch().then(function(results){
            d.resolve(results.filter(function(res){
                return res[key] === range[key];
            })[0]);
        }, function(){
            throw new Error();
        });
        return d.promise;

    }
    else{
        return new Query(this, alias, hash, range);
    }
};

Model.prototype.insert = function(alias){
    return new Inserter(this, alias);
};

Model.prototype.update = function(alias, hash, range){
    var q =  new UpdateQuery(this, alias, hash);
    if(range){
        q.range = range;
    }
    return q;
};

Model.prototype.batch = function(){
    return new Batch(this);
};


// Actually connect to dynamo or magneto.
Model.prototype.getDB = function(key, secret){
    if(this.db !== null){
        return this.db;
    }

    var self = this;
    if(!key || !secret){
        log.warn('Calling connect without key/secret?');
    }
    aws.connect({'key': key, 'secret': secret});
    this.db = aws.dynamo;

    aws.dynamo.on('retry', function(err){
        log.warn('Retrying because of err ' + err);
        self.emit('retry', err);
    })
    .on('successful retry', function(err){
        log.warn('Retry suceeded after encountering error ' + err);
        self.emit('successful', err);
    })
    .on('stat', function(data){
        log.silly(data.action + ' consumed ' + data.consumed + ' units.');
        self.emit('stat', data);
    });

    log.debug('Dynamo client created.');

    if(process.env.MAMBO_BACKEND === "magneto"){
        log.debug('Using magneto');
        this.db.port = process.env.MAGNETO_PORT || 8081;
        this.db.host = "localhost";
        this.db.protocol = 'http';
        log.debug('Connected to magneto on ' +this.db.host+ ':' + this.db.port);
    }
    return this.db;
};

Model.prototype.connect = function(key, secret, prefix, region){
    log.debug('Connecting...');
    var self = this;

    this.prefix = prefix;
    this.region = region;
    this.getDB(key, secret);

    log.debug('Reading schemas...');
    this.schemas.forEach(function(schema){
        var tableName = (self.prefix || "") + schema.tableName;
        schema.tableName = tableName;
        self.schemasByAlias[schema.alias] = schema;
        self.tablesByName[tableName] = schema;
    });

    this.connected = true;
    log.debug('Ready.  Emitting connect.');

    this.emit('connect');
    return this;
};

// Create all tables as defined by this models schemas.
Model.prototype.createAll = function(){
    var d = when.defer();
    when.all(Object.keys(this.schemasByAlias).map(this.ensureTableExists.bind(this)),
        d.resolve);

    return d.promise;
};

// Check if a table already exists.  If not, create it.
Model.prototype.ensureTableExists = function(alias){
    var self = this;
    log.silly('Making sure table `' + alias + '` exists');
    return this.getDB().listTables().then(function(data){
        if(data.TableNames.indexOf(self.schema(alias).tableName) !== -1){
            log.silly('Table already exists ' + alias);
            return false;
        }
        var schema = self.schema(alias);
        log.silly('Table doesnt exist.  Creating...');
        var req = {
            'TableName': schema.tableName,
            'KeySchema': schema.schema,
            'ProvisionedThroughput':{
                'ReadCapacityUnits':5,
                'WriteCapacityUnits':10
            }
        };
        log.silly('Calling to create ' + JSON.stringify(req));
        return self.db.createTable(req);
    });
};

// Low level get item wrapper.
// Params:
// - alias: The table alias name
// - hash: the value of the key-hash of the object you want to retrieve, eg:
// - the song ID
// - range: the value of the key-range of the object you want to retrieve
// - attributesToGet: An array of names of attributes to return in each
// - object. If empty, get all attributes.
// - consistentRead: boolean
Model.prototype.get = function(alias, hash, range, attributesToGet, consistentRead){
    var d = when.defer(),
        schema = this.schema(alias),
        request;
    log.debug('Get `'+alias+'` with hash `'+hash + ((range !== undefined) ? '` and range `'+range+'`': ''));

    // Assemble the request data
    request = {
        'TableName': schema.tableName,
        'Key': schema.exportKey(hash, range)
    };

    if(attributesToGet && attributesToGet.length > 0){
        request.AttributesToGet = attributesToGet;
    }

    if(consistentRead){
        request.ConsistentRead = consistentRead;
    }

    log.silly('Built GET_ITEM request: ' + util.inspect(request, false, 5));

    this.getDB().getItem(request).then(function(data){
        log.silly('GET_ITEM returned: data: ' + util.inspect(data, false, 5));
        return d.resolve((data.Item !== undefined) ?
                this.schema(alias).import(data.Item) : null);
    }.bind(this), function(err){
        log.error('GET_ITEM: ' + err.message + '\n' + err.stack);
        return d.reject(err);
    });

    return d.promise;
};

// Lowlevel delete item wrapper
// example:
//     delete('alias', 'hash', {
//          'range': 'blahblah',
//          'expectedValues': [{
//              'attributeName': 'attribute_name',
//              'expectedValue': 'current_value', // optional
//              'exists': 'true' // defaults to true
//            }],
//          'returnValues':  'NONE'
//        })
Model.prototype.delete = function(alias, hash, opts){
    opts = opts || {};

    log.debug('Delete `'+alias+'` with hash `'+hash+'` and range `'+opts.range+'`');

    var schema = this.schema(alias),
        request = {
            'TableName': schema.tableName,
            'Key': schema.exportKey(hash, opts.range),
            'ReturnValues': opts.returnValues || 'NONE'
        };

    // Add expectedValues for conditional delete
    if(opts.expectedValues){
        request.Expected = {};
        opts.expectedValues.forEach(function(attr){
            var expectedAttribute = {
                    'Exists': attr.exists || Number(true)
                },
                field = schema.field(attr.attributeName);

            if (attr.expectedValue) {
                expectedAttribute.Value = {};
                expectedAttribute.Value[field.type] = field.export(attr.expectedValue);
            }
            request.Expected[attr.attributeName] = expectedAttribute;
        });
    }
    log.silly('Built DELETE_ITEM request: ' + util.inspect(request, false, 5));

    // Make the request
    return this.getDB().deleteItem(request).then(function(data){
        log.silly('DELETE_ITEM returned: ' + util.inspect(data, false, 5));
        return data;
    }.bind(this), function(err){
        log.error('DELETE_ITEM: ' + err.message + '\n' + err.stack);
        return err;
    });
};

var sortObjects = function(objects, values, property){
    property = property || 'id';

    var objectMap = {},
        i = 0;

    for(i=0; i < objects.length; i++){
        objectMap[objects[i][property]] = objects[i];
    }

    return values.map(function(value){
        return objectMap[value] || null;
    }).filter(function(o){
        return o !== null;
    });
};


// Accepts an array of objects
// Each object should look like this:
// {
//     'alias': 'url',
//     'hashes': [2134, 1234],
//     'ranges': [333333, 222222],
//     'attributesToGet': ['url']
// }
// alias is the table alias name
// hashes is an array of key-hashes of objects you want to get from this table
// ranges is an array of key-ranges of objects you want to get from this table
// only use ranges if this table has ranges in its key schema
// hashes and ranges must be the same length and have corresponding values
// attributesToGet is an array of the attributes you want returned for each
// object. Omit if you want the whole object.

// Example:
// To get the urls of songs 1, 2, and 3 and the entire love objects for
// love 98 with created value 1350490700640 and love 99 with 1350490700650:
// [
//     {
//         'alias': 'song',
//         'hashes': [1, 2, 3],
//         'attributesToGet': ['url']
//     },
//     {
//         'alias': 'loves',
//         'hashes': [98, 99],
//         'ranges': [1350490700640, 1350490700650]
//     },
// ]
Model.prototype.batchGet = function(req){
    log.debug('Batch get ' + util.inspect(req, false, 5));
    var request = {
            'RequestItems': {}
        },
        results = {},
        schema,
        obj;

    // Assemble the request data
    req.forEach(function(item){
        item.ranges = item.ranges || [];

        schema = this.schema(item.alias);
        request.RequestItems[schema.tableName] = {'Keys': []};
        request.RequestItems[schema.tableName].Keys = item.hashes.map(function(hash, index){
            return schema.exportKey(hash, item.ranges[index]);
        });

        // Add attributesToGet
        if(item.attributesToGet){
            request.RequestItems[schema.tableName].AttributesToGet = item.attributesToGet;
        }
    }.bind(this));

    log.silly('Built DELETE_ITEM request: ' + util.inspect(request, false, 5));

    // Make the request
    return this.getDB().batchGetItem(request).then(function(data){
        log.silly('BATCH_GET returned: ' + util.inspect(data, false, 5));

        // translate the response from dynamo format to exfm format
        req.forEach(function(tableData){
            var schema = this.schema(tableData.alias),
                items = data.Responses[schema.tableName].Items;

            results[tableData.alias] = items.map(function(dynamoObj){
                return schema.import(dynamoObj);
            }.bind(this));

            // Sort the results
            results[tableData.alias] = sortObjects(results[tableData.alias],
                tableData.hashes, schema.hash);

        }.bind(this));
        return results;
    }.bind(this), function(err){
        log.error('BATCH_GET: ' + err.message + '\n' + err.stack);
        return err;
    });
};


// this.batchWrite(
//     {
//         'song': [
//             {
//                 'id': 1,
//                 'title': 'Silence in a Sweater'
//             },
//             {
//                 'id': 2,
//                 'title': 'Silence in a Sweater (pt 2)'
//             },
//         ]
//     },
//     {
//         'song': [
//             {'id': 3}
//         ]
//     }
// );
Model.prototype.batchWrite = function(puts, deletes){
    log.debug('Batch write: puts`'+util.inspect(puts, false, 10)+'`, deletes`'+util.inspect(deletes, false, 10)+'` ');
    var self = this,
        req = {
            'RequestItems': {}
        },
        totalOps = 0;

    Object.keys(puts).forEach(function(alias){
        var schema = this.schema(alias);

        if(!req.RequestItems.hasOwnProperty(schema.tableName)){
            req.RequestItems[schema.tableName] = [];
        }
        puts[alias].forEach(function(put){
            req.RequestItems[schema.tableName].push({
                'PutRequest': {
                    'Item': schema.export(put)
                }
            });
            totalOps++;
        });
    }.bind(this));

    Object.keys(deletes).forEach(function(alias){
        var schema = this.schema(alias);

        if(!req.RequestItems.hasOwnProperty(schema.tableName)){
            req.RequestItems[schema.tableName] = [];
        }

        deletes[alias].forEach(function(del){
            var range = schema.range ? del[schema.range] : undefined;
            req.RequestItems[schema.tableName].push({
                'DeleteRequest': {
                    'Key': schema.exportKey(del[schema.hash], range)
                }
            });
            totalOps++;
        });
    }.bind(this));

    if(totalOps > 25){
        throw new Error(totalOps + ' is too many for one batch!');
    }

    log.silly('Built BATCH_WRITE request: ' + util.inspect(req, false, 10));
    return this.getDB().batchWriteItem(req).then(function(data){
        log.silly('BATCH_WRITE returned: ' + util.inspect(data, false, 5));
        var success = {};
        Object.keys(data.Responses).forEach(function(tableName){
            success[self.tableNameToAlias(tableName)] = data.Responses[tableName].ConsumedCapacityUnits;
        });
        return {'success': success,'unprocessed': data.UnprocessedItems};
    }, function(err){
        log.error('BATCH_WRITE: ' + err.message + '\n' + err.stack);
        return err;
    });
};

// http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_PutItem.html

// alias: The table alias name

// obj: The object to put in the table. This method will handle formatting
// the object and casting.
// Sample:
// {
//     "url":"http://thissongexistsforreallzz.com/song1.mp3",
//     "id":30326673248,
//     "url_md5":"66496db3a1bbba45fb189030954e78d0",
//     "metadata_state":"pending",
//     "loved_count":0,
//     "listened":0,
//     "version":1,
//     "created":1350500174375
// }
//

// expected: See AWS docs for an explanation. This method handles casting
// and supplying the attribute types, so this object is somewhat simplified
// from what AWS accepts.
// Sample:
// {
//     'metadata_state': {'Value': 'pending', 'Exists': true},
//     'version': {'Value': 0, 'Exists': true}
// }
// returnValues: See AWS docs for an explanation.
Model.prototype.put = function(alias, obj, expected, returnOldValues){
    log.debug('Put `'+alias+'` '+ util.inspect(obj, false, 10));
    var request,
        schema = this.schema(alias),
        clean = schema.export(obj);

    request = {'TableName': schema.tableName, 'Item': schema.export(obj)};

    if(expected && Object.keys(expected).length > 0){
        request.Expected = {};
        Object.keys(expected).forEach(function(key){
            var field = schema.field(key);
            request.Expected[key] = {
                'Value': {}
            };
            request.Expected[key].Exists = expected[key].Exists;
            if(expected[key].Value !== undefined){
                request.Expected[key].Value[field.type] = field.export(expected[key].Value);
            }
        }.bind(this));
    }

    if(returnOldValues === true){
        request.ReturnValues = "ALL_OLD";
    }

    log.silly('Built PUT request: ' + util.inspect(request, false, 10));

    // Make the request
    return this.getDB().putItem(request).then(function(data){
        log.silly('PUT returned: ' + util.inspect(data, false, 5));
        return obj;
    }, function(err){
        log.error('PUT: ' + err.message + '\n' + err.stack);
        return err;
    });
};

// usage:
// update('alias', 'hash', [{
//      'attributeName': 'attribute_name'
//      'newValue': 'new_value',
//      'action': 'PUT'
//    }], {
//      'range': 'blahblah',
//      'expectedValues': [{
//          'attributeName': 'attribute_name',
//          'expectedValue': 'current_value', // optional
//          'exists': 'true' // defaults to true
//        }],
//      'returnValues':  'NONE'
//    })
Model.prototype.updateItem = function(alias, hash, attrs, opts){
    opts = opts || {};

    log.debug('Update `'+alias+'` with hash `'+hash + '`' +
        ((opts.range !== undefined) ? ' and range `'+opts.range+'` ': ' ') +
        ' do => ' + util.inspect(attrs, false, 5));

    var d = when.defer(),
        response = [],
        schema = this.schema(alias),
        request = {
            'TableName': schema.tableName,
            'Key': schema.exportKey(hash, opts.range),
            'AttributeUpdates': {},
            'ReturnValues': opts.returnValues || 'NONE'
        },
        obj,
        expectedAttributes = {},
        expectedAttribute = {};


    // Add attributeUpdates
    attrs.forEach(function(attr){
        // if(attr.attributeName != schema.hash && attr.attributeName != schema.range){
            var field = schema.field(attr.attributeName),
                attributeUpdate = {
                    'Action': attr.action || 'PUT'
                };
            if(!field){
                throw new Error('Unknown field ' + attr.attributeName);
            }

            if(attr.newValue !== undefined){
                attributeUpdate.Value = {};
                attributeUpdate.Value[field.type] = field.export(attr.newValue);
            }

            request.AttributeUpdates[attr.attributeName] = attributeUpdate;
        // }
    }.bind(this));

    // Add expectedValues for conditional update
    if(opts.expectedValues !== undefined){
        request.Expected = {};
        opts.expectedValues.forEach(function(attr){
            var field = schema.field(attr.attributeName);
            expectedAttribute = {
                'Exists': Number(attr.exists).toString()
            };
            if (attr.expectedValue) {
                expectedAttribute.Value = {};
                expectedAttribute.Value[field.type] = field.export(attr.expectedValue);
            }

            request.Expected[attr.attributeName] = expectedAttribute;
        });
    }

    log.silly('Built UPDATE_ITEM request: ' + util.inspect(request, false, 10));

    // Make the request
    this.getDB().updateItem(request).then(function(data){
        log.silly('UPDATE_ITEM returned: ' + util.inspect(data, false, 5));
        if (opts.returnValues !== undefined) {
            return d.resolve(schema.import(data.Attributes));
        }
        d.resolve(data);
    }, function(err){
        log.error('UPDATE_ITEM: ' + err.message + ((err.stack) ? '\n' + err.stack: ''));
        d.reject(err);
    });
    return d.promise;
};

// usage:
// query('alias', 'hash', {
//     'limit': 2,
//     'consistentRead': true,
//     'scanIndexForward': true,
//     'rangeKeyCondition': {
//         'attributeValueList': [{
//             'attributeName': 'blhah',
//             'attributeValue': 'some_value'
//         }],
//         'comparisonOperator': 'GT'
//     },
//     'exclusiveStartKey': {
//         'hashName': 'some_hash',
//         'rangeName': 'some_range'
//     },
//     'attributeToGet':  ['attribute']
// })
Model.prototype.query = function(alias, hash, opts){
    opts = opts || {};

    log.debug('Query `'+alias+'` with hash `'+hash+'` and range `'+opts.range+'`');
    log.silly('Query options: ' + util.inspect(opts, false, 5));

    var response = [],
        schema = this.schema(alias),
        request = {
            'TableName': schema.tableName
        },
        obj,
        hashKey = {},
        rangeKey = {},
        attributeValueList = [],
        attributeValue = {},
        exclusiveStartKey = {},
        attr,
        dynamoType,
        filteredItem;

    // Add HashKeyValue
    hashKey[schema.hashType] = hash.toString();
    request.HashKeyValue = hashKey;

    // Add RangeKeyCondition
    if(opts.rangeKeyCondition !== undefined){
        request.RangeKeyCondition = opts.rangeKeyCondition;
    }

    // Add Limit
    if(opts.limit !== undefined){
        request.Limit = Number(opts.limit);
    }

    // Add ConsistentRead
    if(opts.consistentRead){
        request.ConsistentRead = opts.consistentRead;
    }

    // Add ScanIndexForward
    if(opts.scanIndexForward !== undefined){
        request.ScanIndexForward = opts.scanIndexForward;
    }

    // Add ExclusiveStartKey
    if(opts.exclusiveStartKey !== undefined){
        hashKey[schema.hashType] = opts.exclusiveStartKey.hashName.toString();
        request.ExclusiveStartKey.HashKeyElement = hashKey;
        if(opts.exclusiveStartKey.range !== undefined){
            rangeKey[schema.rangeType] = opts.exclusiveStartKey.rangeName.toString();
            request.ExclusiveStartKey.RangeKeyElement = rangeKey;
        }
    }

    // Add AttributesToGet
    if(opts.attributesToGet !== undefined){
        request.AttributesToGet = opts.attributesToGet;
    }

    log.silly('Built QUERY request: ' + util.inspect(request, false, 10));

    // Make the request
    return this.getDB().query(request).then(function(data){
        log.silly('QUERY returned: ' + util.inspect(data, false, 5));

        return data.Items.map(function(item){
            // Cast the raw data from dynamo
            item = schema.import(item);
            if(opts.attributesToGet){
                // filter out attributes not in attributesToGet
                filteredItem = {};
                Object.keys(item).forEach(function(key){
                    if(opts.attributesToGet.indexOf(key) !== -1){
                        filteredItem[key] = item[key];
                    }
                });
                item = filteredItem;
            }
            return item;
        });
    }, function(err){
        log.error('QUERY: ' + err.message + '\n' + err.stack);
        return err;
    });
};

Model.prototype.scan = function(alias){
    return new Scanner(this, alias);
};


Model.prototype.runScan = function(alias, filter, opts){
    var self = this,
        schema = this.schema(alias),
        req = {
            'TableName': schema.tableName,
            'ScanFilter': {}
        };

    if(opts.limit !== undefined){
        req.Limit = opts.limit;
    }

    if(opts.startKey !== undefined){
        req.ExclusiveStartKey = schema.exportKey(opts.startKey);
    }

    if(opts.count !== undefined && opts.fields !== undefined){
        throw new Error('Can\'t specify count and fields in the same scan.');
    }

    if(opts.count !== undefined){
        req.Count = opts.count;
    }

    if(opts.fields !== undefined){
        req.AttributesToGet = opts.fields;
    }

    Object.keys(filter).forEach(function(key){
        var f = new Scanner.Filter(schema, key, filter[key]);
        req.ScanFilter[key] = f.export();

    });
    log.silly('Built SCAN request: ' + util.inspect(req, false, 10));

    // Make the request
    return this.getDB().scan(req).then(function(data){
        log.silly('SCAN returned: ' + util.inspect(data, false, 5));
        return new Scanner.ScanResult(self, alias, data);
    }, function(err){
        log.error('SCAN: ' + err.message);
        return err;
    });
};


Model.prototype.waitForTableStatus = function(alias, status){
    var d = when.defer(),
        self = this,
        tableName = this.schema(alias).tableName;

    this.getDB().describeTable({'TableName': tableName}).then(function(data){
        if(status === 'DELETED' && !data){
            return d.resolve(true);
        }
        if(data && data.Table.TableStatus === status){
            return d.resolve(true);
        }
        setTimeout(function(){
            self.waitForTableStatus(alias, status).then(function(_){
                d.resolve(_);
            });
        }, 50);
    });
    return d.promise;
};

Model.prototype.waitForTableDelete = function(alias){
    return this.waitForTableStatus(alias, 'DELETED');
};

Model.prototype.waitForTableCreation = function(alias){
    return this.waitForTableStatus(alias, 'ACTIVE');
};

Model.prototype.deleteTable = function(alias){
    return this.getDB().deleteTable({'TableName': this.schema(alias).tableName});
};

Model.prototype.createTable = function(alias, read, write){
    read = read || 10;
    write = write || 10;

    var schema = this.schema(alias);

    return this.getDB().createTable({
        'TableName': schema.tableName,
        'KeySchema': schema.getKeySchema(),
        'ProvisionedThroughput': {
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write
        }
    });
};
module.exports.Model = Model;
module.exports.Schema = Schema;
Object.keys(fields).forEach(function(fieldName){
    module.exports[fieldName] = fields[fieldName];
});
module.exports.instances = instances;

module.exports.connect = function(key, secret, prefix, region){
    instances.forEach(function(instance){
        instance.connect(key, secret, prefix, region);
    });
};

module.exports.createAll = function(){
    return when.all(instances.map(function(instance){
        return instance.createAll();
    }));
};


module.exports.testing = function(){
    return function(){
        var magneto = require('magneto');

        magneto.server = null;
        // plog.find(/magneto*/).level('silly');

        process.env.MAMBO_BACKEND = 'magneto';

        module.exports.recreateTable = function(instance, alias){
            return instance.deleteTable(alias).then(function(){
                return instance.createTable(alias);
            });
        };

        // Drop all tables for all instances and rebuild them.
        module.exports.recreateAll = function(){
            return when.all(instances.map(function(instance){
                return when.all(Object.keys(instance.schemasByAlias).map(function(alias){
                    return module.exports.recreateTable(instance, alias);
                }));
            }));
        };

        module.exports.dropAll = function(){
            return when.all(instances.map(function(instance){
                return when.all(Object.keys(instance.schemasByAlias).map(function(alias){
                    return instance.deleteTable(alias);
                }));
            }));
        };

        module.exports.testing.before = function(done){
            function onReady(){
                log.debug('Recreating all tables for testing...');
                module.exports.createAll().then(function(){
                    if(done){
                        return done();
                    }
                    return true;
                });
            }
            if(magneto.server){
                return onReady();
            }
            log.debug('Starting magneto on port 8081...');
            magneto.server = magneto.listen(8081, function(){
                onReady();
            });
        };

        module.exports.testing.afterEach = function(done){
            return module.exports.recreateAll().then(function(){
                if(done){
                    return done();
                }
                return true;
            });
        };

        module.exports.testing.after = function(done){
            return module.exports.dropAll().then(function(){
                if(done){
                    return done();
                }
                return true;
            });
        };
    };
};

module.exports.use = function(fn){
    fn();
};