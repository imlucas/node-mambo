"use strict";

var aws = require("aws-sdk"),
    async = require('async'),
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
    log = plog('mambo').level('error');

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

    log.debug('Reading schemas...');
    this.schemas.forEach(function(schema){
        var tableName = (this.prefix || "") + schema.tableName;
        schema.tableName = tableName;
        this.schemasByAlias[schema.alias] = schema;
        this.tablesByName[tableName] = schema;
    }.bind(this));

    instances.push(this);

    if(module.exports.lastConnection){
        this.conect.apply(this, module.exports.lastConnection);
    }
}
util.inherits(Model, EventEmitter);

// Grab a schema definition by alias.
Model.prototype.schema = function(alias){
    var s = this.schemasByAlias[alias];
    if(!s){
        throw new Error('Counldn\'t find schema for `'+alias+
            '`.  Did you mistype or forget to register your schema?');
    }
    return s;
};

Model.prototype.tableNameToAlias = function(name){
    return this.tablesByName[name].alias;
};

// Fetch a query wrapper Django style.
Model.prototype.objects = function(alias, hash, range, done){
    if(typeof range === 'object'){
        var key = Object.keys(range)[0],
            q = new Query(this, alias, hash);

        q.fetch(function(err, results){
            if(err){
                return done(err);
            }
            done(null, results.filter(function(res){
                return res[key] === range[key];
            })[0]);
        });

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


    log.debug('Dynamo client created.');

    if(process.env.MAMBO_BACKEND === "magneto"){
        log.debug('Using magneto');
        magneto.patchClient(aws, process.env.MAGNETO_PORT || 8081);
        log.debug('Connected to magneto on localhost:' + ( process.env.MAGNETO_PORT || 8081));
    }
    else {
        if(!key || !secret){
            log.warn('Calling connect without key/secret?');
        }
        else {
            aws.config.update({'accessKeyId': key, 'secretAccessKey': secret});
        }
    }

    this.db = new aws.DynamoDB();
    // this.db.on('retry', function(req){
    //     self.emit('retry', req);
    // })
    // .on('successful retry', function(req){
    //     self.emit('successful retry', req);
    // })
    // .on('retries exhausted', function(req){
    //     self.emit('retries exhausted', req);
    // })
    // .on('stat', function(data){
    //     self.emit('stat', data);
    // });
    return this.db;
};

Model.prototype.connect = function(key, secret, prefix, region){
    log.debug('Connecting...');
    var self = this;

    key = key || process.env.AWS_ACCESS_KEY;
    secret = secret || process.env.AWS_SECRET_KEY;
    region = region || process.env.AWS_REGION || 'us-east-1';
    prefix = prefix || process.env.MAMBO_PREFIX || '';

    this.prefix = prefix;
    this.region = region;
    this.getDB(key, secret);

    this.connected = true;
    log.debug('Ready.  Emitting connect.');

    this.emit('connect');
    return this;
};

// Create all tables as defined by this models schemas.
Model.prototype.createAll = function(done){
    var self = this;
    async.parallel(Object.keys(this.schemasByAlias).map(function(alias){
        return function(callback){
            self.ensureTableExists(alias, callback);
        };
    }), done);
};

// Check if a table already exists.  If not, create it.
Model.prototype.ensureTableExists = function(alias, done){
    var self = this;
    log.silly('Making sure table `' + alias + '` exists');

    this.getDB().listTables(function(err, data){
        if(err){
            return done(err);
        }

        if(data.TableNames.indexOf(self.schema(alias).tableName) !== -1){
            log.silly('Table already exists ' + alias);
            return done(null);
        }

        log.silly('Table doesnt exist.  Creating...');

        var schema = self.schema(alias),
            req = {
                'TableName': schema.tableName,
                'KeySchema': schema.schema,
                'ProvisionedThroughput':{
                    'ReadCapacityUnits':5,
                    'WriteCapacityUnits':10
                }
            };
        log.silly('Calling to create ' + JSON.stringify(req));
        self.db.createTable(req, done);
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
Model.prototype.get = function(alias, hash, range, attrs, consistent, done){
    var schema = this.schema(alias),
        request;

    log.debug('Get `'+alias+'` with hash `'+hash +
        ((range !== undefined) ? '` and range `'+range+'`': ''));

    // Assemble the request data
    request = {
        'TableName': schema.tableName,
        'Key': schema.exportKey(hash, range)
    };

    if(attrs && attrs.length > 0){
        request.AttributesToGet = attrs;
    }

    if(consistent){
        request.ConsistentRead = consistent;
    }

    log.silly('Built GET_ITEM request: ' + util.inspect(request, false, 5));

    this.getDB().getItem(request, function(err, data){
        if(err){
            log.error('GET_ITEM: ' + err.message + '\n' + err.stack);
            return done(err);
        }
        log.silly('GET_ITEM returned: data: ' + util.inspect(data, false, 5));
        return done(null, (data.Item !== undefined) ?
                this.schema(alias).import(data.Item) : null);
    });

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
Model.prototype.delete = function(alias, hash, opts, done){
    opts = opts || {};

    log.debug('Delete `'+alias+'` with hash `'+hash+'` and range `'+opts.range+'`');

    var self = this,
        schema = this.schema(alias),
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
    this.getDB().deleteItem(request, function(err, data){
        if(err){
            log.error('DELETE_ITEM: ' + err.message + '\n' + err.stack);
            return done(err);
        }

        log.silly('DELETE_ITEM returned: ' + util.inspect(data, false, 5));
        self.emit('delete', [alias, hash, opts.range]);
        done(null, data);
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
Model.prototype.batchGet = function(req, done){
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
    this.getDB().batchGetItem(request, function(err, data){
        if(err){
            log.error('BATCH_GET: ' + err.message + '\n' + err.stack);
            return done(err);
        }

        log.silly('BATCH_GET returned: ' + util.inspect(data, false, 5));

        // translate the response from dynamo format to exfm format
        req.forEach(function(tableData){
            var schema = this.schema(tableData.alias),
                items = data.Responses[schema.tableName].Items;

            results[tableData.alias] = items.map(schema.import);

            // Sort the results
            results[tableData.alias] = sortObjects(results[tableData.alias],
                tableData.hashes, schema.hash);

        });
        done(null, results);
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
Model.prototype.batchWrite = function(puts, deletes, done){
    log.debug('Batch write: puts`'+util.inspect(puts, false, 10)+'`, deletes`'+util.inspect(deletes, false, 10)+'` ');
    var self = this,
        req = {
            'RequestItems': {}
        },
        totalOps = 0;

    Object.keys(puts).forEach(function(alias){
        var schema = self.schema(alias);

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
    });

    Object.keys(deletes).forEach(function(alias){
        var schema = self.schema(alias);

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
    this.getDB().batchWriteItem(req, function(err, data){
        if(err){
            log.error('BATCH_WRITE: ' + err.message + '\n' + err.stack);
            return done(err);
        }
        log.silly('BATCH_WRITE returned: ' + util.inspect(data, false, 5));
        var success = {};
        Object.keys(data.Responses).forEach(function(tableName){
            success[self.tableNameToAlias(tableName)] = data.Responses[tableName].ConsumedCapacityUnits;
        });
        done(null, {'success': success,'unprocessed': data.UnprocessedItems});
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
Model.prototype.put = function(alias, obj, expected, returnOldValues, done){
    log.debug('Put `'+alias+'` '+ util.inspect(obj, false, 10));
    var self = this,
        request,
        schema = this.schema(alias),
        clean = schema.export(obj);

    request = {'TableName': schema.tableName, 'Item': schema.export(obj)};

    if(expected && Object.keys(expected).length > 0){
        request.Expected = {};
        Object.keys(expected).forEach(function(key){
            var field = schema.field(key);
            request.Expected[key] = {};
            request.Expected[key].Exists = expected[key].Exists;
            if(expected[key].Value !== undefined){
                request.Expected[key].Value = {};
                request.Expected[key].Value[field.type] = field.export(expected[key].Value);
            }
        });
    }

    if(returnOldValues === true){
        request.ReturnValues = "ALL_OLD";
    }

    log.silly('Built PUT request: ' + util.inspect(request, false, 10));

    // Make the request
    this.getDB().putItem(request, function(err, data){
        if(err){
            log.error('PUT: ' + err.message + (err.stack ? '\n' + err.stack : ''));
            return done(err);
        }
        log.silly('PUT returned: ' + util.inspect(data, false, 5));
        self.emit('insert', {
            'alias': alias,
            'expected': expected,
            'data': obj
        });
        done(null, obj);
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
Model.prototype.updateItem = function(alias, hash, attrs, opts, done){
    opts = opts || {};

    log.debug('Update `'+alias+'` with hash `'+hash + '`' +
        ((opts.range !== undefined) ? ' and range `'+opts.range+'` ': ' ') +
        ' do => ' + util.inspect(attrs, false, 5));

    var self = this,
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
    this.getDB().updateItem(request, function(err, data){
        if(err){
            log.error('UPDATE_ITEM: ' + err.message + ((err.stack) ? '\n' + err.stack: ''));
            return done(err);
        }
        log.silly('UPDATE_ITEM returned: ' + util.inspect(data, false, 5));
        self.emit('update', {
            'alias': alias,
            'range': opts.range,
            'updates': attrs,
            'options': opts
        });
        if (opts.returnValues !== undefined) {
            return done(null, schema.import(data.Attributes));
        }
        done(null, data);
    });
};

// usage:
// query('alias', 'hash', {
//     'limit': 2,
//     'consistentRead': true,
//     'scanIndexForward': true,
//     'conditions': {
//         'blah': {'GT': 'some_value'}
//     },
//     'exclusiveStartKey': {
//         'hashName': 'some_hash',
//         'rangeName': 'some_range'
//     },
//     'attributeToGet':  ['attribute']
// })
Model.prototype.query = function(alias, hash, opts, done){
    opts = opts || {};

    log.debug('Query `'+alias+'` with hash `'+hash+'` and range `'+opts.range+'`');
    log.silly('Query options: ' + util.inspect(opts, false, 5));

    var response = [],
        schema = this.schema(alias),
        request = {
            'TableName': schema.tableName,
            'KeyConditions': {}
        },
        obj,
        hashKey = {},
        rangeKey = {},
        attributeValueList = [],
        attributeValue = {},
        exclusiveStartKey = {},
        attr,
        dynamoType,
        filteredItem,
        hashField = schema.field(schema.hash);

    function addKeyCondition(key, op, vals){
        var field = schema.field(key);
        if(!Array.isArray(vals)){
            vals = [vals];
        }

        request.KeyConditions[key] = {
            'AttributeValueList': [],
            'ComparisonOperator': op
        };
        vals.forEach(function(val){
            var i = {};
            i[field.type] = field.export(val);
            request.KeyConditions[key].AttributeValueList.push(i);
        });
    }

    addKeyCondition(schema.hash, 'EQ', hash);

    if(opts.conditions){
        Object.keys(opts.conditions).forEach(function(key){
            var field = schema.field(key),
                op = Object.keys(opts.conditions[key])[0],
                vals = opts.conditions[key][op];
            addKeyCondition(key, op, vals);
        });
    }

    if(opts.index){
        request.IndexName = opts.index;
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
    this.getDB().query(request, function(err, data){
        if(err){
            log.error('QUERY: ' + err.message + '\n' + err.stack);
            return done(err);
        }
        log.silly('QUERY returned: ' + util.inspect(data, false, 5));

        done(null, data.Items.map(function(item){
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
        }));
    });
};

Model.prototype.scan = function(alias){
    return new Scanner(this, alias);
};


Model.prototype.runScan = function(alias, filter, opts, done){
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
        console.log(opts.count, opts.fields);
        console.error(new Error('Can\'t specify count and fields in the same scan.'));
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
    this.getDB().scan(req, function(err, data){
        if(err){
            log.error('SCAN: ' + err.message);
            return done(err);
        }
        log.silly('SCAN returned: ' + util.inspect(data, false, 5));
        done(null, new Scanner.ScanResult(self, alias, data, filter, opts));
    });
};


Model.prototype.waitForTableStatus = function(alias, status, done){
    var self = this,
        tableName = this.schema(alias).tableName;

    this.getDB().describeTable({'TableName': tableName}, function(err, data){
        if(status === 'DELETED' && !data){
            return done(null, true);
        }
        if(data && data.Table.TableStatus === status){
            return done(null, true);
        }
        setTimeout(function(){
            self.waitForTableStatus(alias, status, done);
        }, 50);
    });
};

Model.prototype.waitForTableDelete = function(alias){
    return this.waitForTableStatus(alias, 'DELETED');
};

Model.prototype.waitForTableCreation = function(alias){
    return this.waitForTableStatus(alias, 'ACTIVE');
};

Model.prototype.deleteTable = function(alias, done){
    var self = this;

    this.getDB().deleteTable({
        'TableName': this.schema(alias).tableName
    }, function(err, res){
        self.emit('delete table', alias);
        done(err, res);
    });
};

Model.prototype.createTable = function(alias, read, write, done){
    read = read || 10;
    write = write || 10;

    var schema = this.schema(alias),
        self = this;

    return this.getDB().createTable({
        'TableName': schema.tableName,
        'KeySchema': schema.getKeySchema(),
        'ProvisionedThroughput': {
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write
        }
    }, function(err, res){
        self.emit('create table', alias, read, write);
        done(err, res);
    });
};

Model.prototype.updateHash = function(alias, oldHash, newHash, includeLinks, done){
    var self = this,
        schema = self.schema(alias);

    function exec(batch){
        if(!batch){
            batch = self.batch();
        }
        return self.get(alias, oldHash, function(err, obj){
            obj[schema.hash] = newHash;
            batch.remove(alias, oldHash)
                .insert(alias, obj)
                .commit(done);
        });
    }
    if(includeLinks){
        return this.updateLinks(alias, oldHash, newHash, true, function(err){
            if(err){
                return done(err);
            }
            exec();
        });
    }
    return exec();
};
// @todo (lucas) Need some serious de-promising.
// Model.prototype.updateLinks = function (alias, oldHash, newHash, returnBatch, done){
//     var self = this,
//         schema = self.schema(alias),
//         batch = self.batch();

//     log.debug('Updating links for `'+alias+'` from `'+oldHash+'` to `'+newHash+'`');
//     if(Object.keys(schema.links).length === 0){
//         log.warn('No links for `'+alias+'`.  Did you mean to call this?');
//         return done();
//     }
//     log.debug('Links: ' + util.inspect(schema.links));

//     return Q.all(Object.keys(schema.links).map(function(linkAlias){
//         log.debug('Getting all `'+alias+'` links to `'+linkAlias+'`');

//         var linkKey = schema.links[linkAlias],
//             rangeKey = Schema.get(linkAlias).range;

//         return self.objects(linkAlias, oldHash).fetch(function(err, docs){
//             log.debug('Got ' + docs.length + ' links');
//             docs.map(function(doc){
//                 doc[linkKey] = newHash;
//                 if(rangeKey){
//                     batch.remove(linkAlias, oldHash, doc[rangeKey]);
//                 }
//                 else{
//                     batch.remove(linkAlias, oldHash);
//                 }
//                 batch.insert(linkAlias, doc);
//             });
//         });
//     }))
//     .then(function(){
//         if(returnBatch){
//             return done(null, batch);
//         }
//         return batch.commit();
//     });
// };

module.exports.Model = Model;
module.exports.Schema = Schema;
Object.keys(fields).forEach(function(fieldName){
    module.exports[fieldName] = fields[fieldName];
});
module.exports.instances = instances;
module.exports.lastConnection = null;

module.exports.connect = function(key, secret, prefix, region){
    module.exports.lastConnection = [key, secret, prefix, region];

    instances.forEach(function(instance){
        instance.connect(key, secret, prefix, region);
    });
};

module.exports.createAll = function(done){
    async.parallel(instances.map(function(instance){
        return function(callback){
            instance.createAll(callback);
        };
    }), done);
};

var magneto = require('magneto');

module.exports.testing = function(opts){
    return function(){
        magneto.server = magneto.server || null;
        // plog.find(/magneto*/).level('silly');

        process.env.MAMBO_BACKEND = 'magneto';

        module.exports.recreateTable = function(instance, alias){
            return instance.deleteTable(alias).then(function(){
                return instance.createTable(alias);
            });
        };

        // Drop all tables for all instances and rebuild them.
        module.exports.recreateAll = function(){
            return Q.all(instances.map(function(instance){
                return Q.all(Object.keys(instance.schemasByAlias).map(function(alias){
                    return module.exports.recreateTable(instance, alias);
                }));
            }));
        };

        module.exports.dropAll = function(){
            return Q.all(instances.map(function(instance){
                return Q.all(Object.keys(instance.schemasByAlias).map(function(alias){
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
                if(magneto.server){
                    log.debug('Stopping magneto');
                    magneto.server.close();
                    magneto.server = null;
                }
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
