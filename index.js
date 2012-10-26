"use strict";

var dynamo = require("dynamo"),
    when = require("when"),
    sequence = require("sequence"),
    winston = require("winston"),
    _ = require("underscore");

// Setup logger
winston.loggers.add("app", {
    console: {
        'level': "silly",
        'timestamp': true,
        'colorize': true
    }
});
var log = winston.loggers.get("app");


// Models have many tables.
// Girths are short hand for throughput.
function Model(tableData){
    this.connected = false;
    this.girths = {};
    this.tables = {};
    this.tablesByName = {};
    this.tableData = tableData;
}

Model.prototype.connect = function(key, secret, prefix, region){
    this.prefix = prefix;
    this.region = region;

    if(process.env.NODE_ENV === "production"){
        // Connect to DynamoDB
        this.client = dynamo.createClient({
            'accessKeyId': key,
            'secretAccessKey': secret
        });
        this.db = this.client.get(this.region || "us-east-1");
    } else {
        if(process.env.TEST_DB_ENV === "dynamo"){
            // Connect to DynamoDB
            this.client = dynamo.createClient({
                'accessKeyId': key,
                'secretAccessKey': secret
            });
            this.db = this.client.get(this.region || "us-east-1");
        }
        else {
            // Connect to Magneto
            this.client = dynamo.createClient();
            this.client.useSession = false;
            this.db = this.client.get(this.region || "us-east-1");
            this.db.host = "localhost";
            this.db.port = process.env.MAGNETO_PORT || 8081;
        }
    }

    this.tableData.forEach(function(table){
        var schema = {},
            typeMap = {
                'N': Number,
                'Number': Number,
                'NS': [Number],
                'NumberSet': [Number],
                'S': String,
                'String': String,
                'SS': [String],
                'StringSet': [String]
            },
            tableName = (this.prefix || "") + table.table,
            t;

        t = this.db.get(tableName);
        t.name = t.TableName;

        t.read = table.read;
        t.write = table.write;
        // The girth attribute is redundant and I'm pretty sure we don't need it.
        t.girth = {
            'read': table.read,
            'write': table.write
        };
        this.girths[table.alias] = t.girth;

        t.hashType = table.hashType;
        t.hashName = table.hashName;
        if(table.rangeName){
            t.rangeType = table.rangeType;
            t.rangeName = table.rangeName;
        }
        t.key = {'HashKeyElement': {}};
        t.key.HashKeyElement[table.hashType] = table.hashName;
        if(table.rangeName){
            t.key.HashKeyElement[table.rangeType] = table.rangeName;
        }

        // Parse table hash and range names and types defined in package.json
        // I believe this is redundant and unused as well.
        schema[table.hashName] = typeMap[table.hashType];
        if (table.rangeName){
            schema[table.rangeName] = typeMap[table.rangeType];
        }
        t.schema = schema;

        t.attributeSchema = this.attributeSchema[table.table];

        this.tables[table.alias] = t;

        this.tablesByName[tableName] = t;

    }.bind(this));

    this.connected = true;
    return this;
};

Model.prototype.table = function(alias){
    return this.tables[alias];
};

Model.prototype.createAll = function(){
    var d = when.defer();
    when.all(Object.keys(this.tables).map(this.ensureTableMagneto.bind(this)), d.resolve);
    return d.promise;
};

Model.prototype.ensureTableMagneto = function(alias){
    var d = when.defer();
    sequence(this).then(function(next){
        this.db.listTables({}, next);
    }).then(function(next, err, data){
        if(!d.rejectIfError(err)){
            next(data);
        }
    }).then(function(next, data){
        if(data.TableNames.indexOf(this.table(alias).name) !== -1){
            return d.resolve(false);
        }
        this.db.add({
            'name': this.table(alias).name,
            'schema': this.table(alias).schema,
            'throughput': this.table(alias).girth
        }).save(function(err, table){
            if(!d.rejectIfError(err)){
                d.resolve(true);
            }
        });
    });
    return d.promise;
};

Model.prototype.get = function(alias, hash, range, attributesToGet, consistentRead){
    // alias: The table alias name

    // hash: the value of the key-hash of the object you want to retrieve, eg:
    // the song ID

    // range: the value of the key-range of the object you want to retrieve

    // attributesToGet: An array of names of attributes to return in each
    // object. If empty, get all attributes.

    // consistentRead: boolean

    var d = when.defer(),
        table = this.table(alias),
        request;

    // Assemble the request data
    request = {
        'TableName': table.name,
        'Key': {
            'HashKeyElement': {}
        }
    };
    request.Key.HashKeyElement[table.hashType] = hash.toString();

    if(table.rangeName){
        request.Key.RangeKeyElement[table.rangeType] = range.toString();
    }
    if(attributesToGet){
        // Get only `attributesToGet`
        request.AttributesToGet = attributesToGet;
    }
    if(consistentRead){
        request.ConsistentRead = consistentRead;
    }

    // Make the request
    this.db.getItem(request, function(err, data){
        if(!err){
            if(data.Item === undefined){
                return d.resolve({});
            }
            return d.resolve(this.fromDynamo(alias, data.Item));
        }
        return d.resolve(err);
    }.bind(this));

    return d.promise;
};

Model.prototype.delete = function(alias, hash, deleteOpts){

    // usage:
    // delete('alias', 'hash', {
    //      'range': 'blahblah',
    //      'expectedValues': [{
    //          'attributeName': 'attribute_name',
    //          'expectedValue': 'current_value', // optional
    //          'exists': 'true' // defaults to true
    //        }],
    //      'returnValues':  'NONE'
    //    })

    var d = when.defer(),
        table = this.table(alias),
        deleteRequest = {},
        opts = {},
        attrSchema = this.table(alias).attributeSchema,
        expectedAttribute = {},
        hashKey = {},
        rangeKey = {};

    if (deleteOpts) {
        opts = deleteOpts;
    }

    deleteRequest = {
        'TableName': table.name,
        'Key': {},
        'ReturnValues': opts.returnValues || 'NONE'
    };

    // Add hash
    hashKey[table.hashType] = hash.toString();
    deleteRequest.Key.HashKeyElement = hashKey;
    // Add range
    if(opts.range){
        rangeKey[table.rangeType] = opts.range.toString();
        deleteRequest.Key.RangeKeyElement = rangeKey;
    }

    // Add expectedValues for conditional delete
    if(opts.expectedValues){
        deleteRequest.Expected = {};
        opts.expectedValues.forEach(function(attr){
            expectedAttribute = {
                'Exists': attr.exists || this.valueToDynamo(true, 'N')
            };
            if (attr.expectedValue) {
                var dynamoType = attrSchema[attr.attributeName].dynamoType;
                expectedAttribute.Value = {};
                expectedAttribute.Value[dynamoType] =
                    this.valueToDynamo(attr.expectedValue, dynamoType);
            }

            deleteRequest.Expected[attr.attributeName] = expectedAttribute;
        }.bind(this));
    }

    // Make the request
    this.db.deleteItem(deleteRequest, function(err, data){
        if(!err){
            return d.resolve(data);
        }
        return d.resolve(err);
    }.bind(this));
    return d.promise;
};

Model.prototype.deleteAllItems = function() {

};

Model.prototype.batchGet = function(req){
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

    var d = when.defer(),
        request,
        results = [],
        table,
        obj;

    // Assemble the request data
    request = {'RequestItems': {}};
    req.forEach(function(item){
        table = this.table(item.alias);
        request.RequestItems[table.name] = {'Keys': []};
        // Add hashes
        item.hashes.forEach(function(hash){
            var hashKey = {'HashKeyElement': {}};
            hashKey.HashKeyElement[table.hashType] = hash.toString();
            request.RequestItems[table.name].Keys.push(hashKey);
        });
        // Add ranges
        if(item.ranges){
            item.ranges.forEach(function(range){
                var rangeKey = {'RangeKeyElement': {}};
                rangeKey.RangeKeyElement[table.rangeType] = range.toString();
                request.RequestItems[table.name].Keys.push(rangeKey);
            });
        }
        // Add attributesToGet
        if(item.attributesToGet){
            request.RequestItems[table.name].AttributesToGet = item.attributesToGet;
        }
    }.bind(this));

    // Make the request
    this.db.batchGetItem(request, function(err, data){
        if(!err){
            // translate the response from dynamo format to exfm format
            req.forEach(function(tableData){
                table = this.table(tableData.alias);

                var items = data.Responses[table.name].Items;
                items.forEach(function(dynamoObj){
                    obj = this.fromDynamo(tableData.alias, dynamoObj);
                    results.push(obj);
                }.bind(this));

                // Sort the results if the ordered flag is true
                if(tableData.ordered){
                    results = this.sortObjects(results, tableData.hashes,
                        table.hashName);
                }
            }.bind(this));
            return d.resolve(results);
        }
        return d.resolve(err);
    }.bind(this));
    return d.promise;
};

var accept = function(item){
    // Returns true if item is not undefined, null, "", [], or {}.
    if(item === false){return true;}
    if(item === 0){return true;}
    if(!item){return false;}
    if((_.isObject(item)) && (_.isEmpty(item))){
        return false;
    }
    return true;
};

Model.prototype.checkForBase36 = function(id){
    if (typeof id === 'string'){
        var idString = id.split(''),
        i,
        validId = true;
        // check if the string contains capitals - if it does, it's invalid
        for (i=0; i<idString.length; i++){
            if (idString[i] === idString[i].toUpperCase() &&
                isNaN(parseInt(idString[i], 10))){
                validId = false;
            }
        }
        if (validId){
            return parseInt(id, 36);
        }
        else {
            return new Error('invalid base36 id');
        }
    }
    return id;
};

Array.prototype.toMap = function(property){
    var m = {},
        i = 0;

    for(i=0; i < this.length; i++){
        m[this[i][property]] = this[i];
    }
    return m;
};

Model.prototype.sortObjects = function(objects, values, property){
    property = property || 'id';

    var objectMap = objects.toMap(property);
    return values.map(function(value){
        return objectMap[value] || null;
    }).filter(function(o){
        return o !== null;
    });
};

Model.prototype.valueToDynamo = function(value, dynamoType, exfmType){
    var newValue;

    if(value === true){
        return "1";
    }
    if(value === false){
        return "0";
    }
    if(dynamoType === "N"){
        return value.toString();
    }
    if(dynamoType === "NS"){
        newValue = [];
        value.forEach(function(item){
            newValue.push(item.toString);
        });
        return newValue;
    }
    if(exfmType === "JSON"){
        return JSON.stringify(value);
    }
    return value;
};

Model.prototype.toDynamo = function(alias, obj){
    var table = this.table(alias),
        dynamoObj = {'TableName': table.name, 'Item': {}};

    Object.keys(obj).map(function(attr){
        if(accept(obj[attr])){
            var dynamoType = table.attributeSchema[attr].dynamoType,
                exfmType = table.attributeSchema[attr].exfmType,
                value = obj[attr];

            dynamoObj.Item[attr] = {};
            dynamoObj.Item[attr][dynamoType] = this.valueToDynamo(value,
                dynamoType, exfmType);
        }
    }.bind(this));
    return dynamoObj;
};

Model.prototype.valueFromDynamo = function(value, dynamoType, exfmType){
    var newValue;
    if(dynamoType === "N"){
        newValue = parseInt(value, 10);
        if(exfmType === "Boolean"){
            if(newValue === 0){
                newValue = false;
            }
            if(newValue === 1){
                newValue = true;
            }
        }
        return newValue;
    }
    if(dynamoType === "NS"){
        newValue = [];
        value.forEach(function(n){
            newValue.push(parseInt(n, 10));
        });
        return newValue;
    }
    if(exfmType === "JSON"){
        return JSON.parse(value);
    }
    return value;
};

Model.prototype.fromDynamo = function(alias, dynamoObj){
    var obj = {};
    Object.keys(dynamoObj).map(function(attr){

        log.debug("attr:", attr);
        log.debug("alias:", alias);
        log.debug("this.table(alias):", this.table(alias));
        log.debug("this.table(alias).attributeSchema:", this.table(alias).attributeSchema);

        var dynamoType = this.table(alias).attributeSchema[attr].dynamoType,
            exfmType = this.table(alias).attributeSchema[attr].exfmType,
            value = dynamoObj[attr][dynamoType];
        obj[attr] = this.valueFromDynamo(value, dynamoType, exfmType);
    }.bind(this));
    return obj;
};

Model.prototype.put = function(alias, obj, expected, returnOldValues){

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

    var d = when.defer(),
        table = this.table(alias),
        request;

    // Assemble the request data
    request = this.toDynamo(alias, obj);
    if(expected){
        request.Expected = {};
        Object.keys(expected).forEach(function(attr){
            var attrType = table.attributeSchema[attr].dynamoType;
            request.Expected[attr] = {};
            request.Expected[attr].Exists = expected[attr].Exists;
            request.Expected[attr].Value = {};

            // Cast values to what dynamo expects
            request.Expected[attr].Value[attrType] = this.valueToDynamo(
                expected[attr].Value, attrType);
        }.bind(this));
    }
    if(returnOldValues === true){
        request.ReturnValues = "ALL_OLD";
    }

    // Make the request
    this.db.putItem(request, function(err, data){
        if(!err){
            return d.resolve(obj);
        }
        return d.resolve(err);
    });
    return d.promise;
};

Model.prototype.updateItem = function(alias, hash, attrs, updateOpts){

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

    var d = when.defer(),
        updateRequest = {},
        response = [],
        table,
        obj,
        hashKey = {},
        rangeKey = {},
        attributeUpdates = {},
        attributeUpdate = {},
        expectedAttributes = {},
        expectedAttribute = {},
        attrSchema = this.table(alias).attributeSchema,
        opts = {};

    table = this.table(alias);
    // updateRequest[table.name] = {};

    if (updateOpts) {
        opts = updateOpts;
    }

    updateRequest = {
        'TableName': table.name,
        'Key': {},
        'AttributeUpdates': {},
        'ReturnValues': opts.returnValues || 'NONE'
    };

    // Add hash
    hashKey[table.hashType] = hash.toString();
    updateRequest.Key.HashKeyElement = hashKey;
    // Add range
    if(opts.range !== undefined){
        rangeKey[table.rangeType] = opts.range.toString();
        updateRequest.Key.RangeKeyElement = rangeKey;
    }
    // Add attributeUpdates
    attrs.forEach(function(attr){
        var dynamoType = attrSchema[attr.attributeName].dynamoType;
        attributeUpdate = {
            'Value': {},
            'Action': attr.action || 'PUT'
        };
        attributeUpdate.Value[dynamoType] = this.valueToDynamo(attr.newValue,
            dynamoType);
        updateRequest.AttributeUpdates[attr.attributeName] = attributeUpdate;
    }.bind(this));
    // Add expectedValues for conditional update
    if(opts.expectedValues !== undefined){
        updateRequest.Expected = {};
        opts.expectedValues.forEach(function(attr){
            var dynamoType = attrSchema[attr.attributeName].dynamoType;
            expectedAttribute = {
                'Exists': attr.exists || this.valueToDynamo(true, 'N')
            };
            if (attr.expectedValue) {
                expectedAttribute.Value = {};
                expectedAttribute.Value[dynamoType] =
                    this.valueToDynamo(attr.expectedValue, dynamoType);
            }

            updateRequest.Expected[attr.attributeName] = expectedAttribute;
        }.bind(this));
    }

    // Make the request
    this.db.updateItem(updateRequest, function(err, data){
        if(!err){
            if (opts.returnValues !== undefined) {
                var fromDynamo = this.fromDynamo(alias, data.Attributes);
                return d.resolve(fromDynamo);
            }
            else {
                return d.resolve(data);
            }
        }
        return d.resolve(err);
    }.bind(this));
    return d.promise;
};

Model.prototype.query = function(alias, hash, queryOpts){

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

    var d = when.defer(),
        queryRequest = {},
        response = [],
        table,
        obj,
        hashKey = {},
        rangeKey = {},
        attributeValueList = [],
        attributeValue = {},
        exclusiveStartKey = {},
        attrSchema = this.table(alias).attributeSchema,
        opts = {},
        attr,
        fromDynamoObjects = [],
        dynamoType;

    table = this.table(alias);
    // updateRequest[table.name] = {};

    if (queryOpts) {
        opts = queryOpts;
    }

    queryRequest = {
        'TableName': table.name
    };

    // Add HashKeyValue
    hashKey[table.hashType] = hash.toString();
    queryRequest.HashKeyValue = hashKey;

    // Add RangeKeyCondition
    if(opts.rangeKeyCondition !== undefined){
        opts.rangeKeyCondition.attributeValueList.forEach(function(attr){
            dynamoType = attrSchema[attr.attributeName];
            attributeValue = {};
            attributeValue[dynamoType] = this.valueToDynamo(
                attr.attributeValue, dynamoType);
            attributeValueList.push(attributeValue);
            // rangeKey[table.rangeType] = attr.attributeValue.toString();
            // updateRequest.Key.RangeKeyElement = rangeKey;
        });
        queryRequest.RangeKeyCondition = {
            'AttributeValueList': attributeValueList,
            'ComparisonOperator': opts.rangeKeyCondition.comparisonOperator
        };
    }

    // Add Limit
    if(opts.limit !== undefined){
        queryRequest.Limit = this.valueToDynamo(opts.limit, "N");
    }

    // Add ConsistentRead
    if(opts.consistentRead !== undefined){
        queryRequest.ConsistentRead = this.valueToDynamo(opts.consistentRead);
    }

    // Add ScanIndexForward
    if(opts.scanIndexForward !== undefined){
        queryRequest.ScanIndexForward = this.valueToDynamo(opts.scanIndexForward);
    }

    // Add ExclusiveStartKey
    if(opts.exclusiveStartKey !== undefined){
        hashKey[table.hashType] = opts.exclusiveStartKey.hashName.toString();
        queryRequest.ExclusiveStartKey.HashKeyElement = hashKey;
        if(opts.exclusiveStartKey.range !== undefined){
            rangeKey[table.rangeType] = opts.exclusiveStartKey.rangeName.toString();
            queryRequest.ExclusiveStartKey.RangeKeyElement = rangeKey;
        }
    }

    // Add AttributesToGet
    if(opts.attributesToGet !== undefined){
        queryRequest.AttributesToGet = opts.attributesToGet;
    }

    // Make the request
    this.db.query(queryRequest, function(err, data){
        if(!err){
            data.Items.forEach(function(item){
                fromDynamoObjects.push(this.fromDynamo(alias, item));
            }.bind(this));
            return d.resolve(fromDynamoObjects);
        }
        return d.resolve(err);
    }.bind(this));
    return d.promise;
};

// Model.prototype.deleteAllItems = function(alias) {
//     var d = when.defer(),
//         table = this.table(alias),
//         scanRequest = {
//             'TableName': table.name
//         },
//         deleteRequest = {
//             'RequestItems': {}
//         };

//     sequence(this).then(function(next){
//         this.db.scan(scanRequest, function(err, data){
//             if(!err){
//                 next(data.Items);
//             }
//             else{
//                 throw new Error(err);
//             }
//         });
//     }).then(function(next, scanResults){

//         var seperatedRequests = [],
//             i,
//             j,
//             chunkSize = 25;

//         // seperate the scan results into chunks
//         for (i = 0, j = scanResults.length; i < j; i += chunkSize) {
//             seperatedRequests.push(scanResults.slice(i, i + chunkSize));
//         }


//         when.all(seperatedRequests.map(function(requests){
//             var p = when.defer(),
//                 deleteRequests = [];
//             requests.forEach(function(item){
//                 deleteRequests.push({
//                     'DeleteRequest': {
//                         'Key': {
//                             'HashKeyElement': item[table.hashName]
//                         }
//                     }
//                 });
//             });
//             deleteRequest.RequestItems[table.name] = deleteRequests;
//             this.db.batchWriteItem(deleteRequest, function(err, data){
//                 p.resolve(true);
//             });
//             return p.promise;
//         }.bind(this)), function(){
//             d.resolve(true);
//         });

//         d.resolve();
//     });

//     return d.promise;
// };

//                     ##      ##    ###    ########  ##    ## #### ##    ##  ######
//  ##   ##   ##   ##  ##  ##  ##   ## ##   ##     ## ###   ##  ##  ###   ## ##    ##   ##   ##   ##   ##
//   ## ##     ## ##   ##  ##  ##  ##   ##  ##     ## ####  ##  ##  ####  ## ##          ## ##     ## ##
// ######### ######### ##  ##  ## ##     ## ########  ## ## ##  ##  ## ## ## ##   #### ######### #########
//   ## ##     ## ##   ##  ##  ## ######### ##   ##   ##  ####  ##  ##  #### ##    ##    ## ##     ## ##
//  ##   ##   ##   ##  ##  ##  ## ##     ## ##    ##  ##   ###  ##  ##   ### ##    ##   ##   ##   ##   ##
//                      ###  ###  ##     ## ##     ## ##    ## #### ##    ##  ######

//                ########     ###    ##    ##  ######   ######## ########   #######  ##     ##  ######
//                ##     ##   ## ##   ###   ## ##    ##  ##       ##     ## ##     ## ##     ## ##    ##
//                ##     ##  ##   ##  ####  ## ##        ##       ##     ## ##     ## ##     ## ##
//                ##     ## ##     ## ## ## ## ##   #### ######   ########  ##     ## ##     ##  ######
//                ##     ## ######### ##  #### ##    ##  ##       ##   ##   ##     ## ##     ##       ##
//                ##     ## ##     ## ##   ### ##    ##  ##       ##    ##  ##     ## ##     ## ##    ##
//                ########  ##     ## ##    ##  ######   ######## ##     ##  #######   #######   ######


Model.prototype.recreateTable = function(alias) {
    var d = when.defer(),
        table = this.table(alias),
        tableRequest = {
            'TableName': table.name
        },
        tableDescription = {};

    // if (process.env.NODE_ENV !== 'testing') {
    //     throw new Error('Can only recreate a table in testing environment');
    // }
    sequence(this).then(function(next){
        this.db.describeTable(tableRequest, function(err, data){
            if (!err) {
                next(data);
            }
            else {
                throw new Error(err);
            }
        });
    }).then(function(next, data){
        tableDescription = data;
        this.db.deleteTable(tableRequest, function(err, data){
            if (!err) {
                next(data);
            }
            else {
                throw new Error(err);
            }
        });
    }).then(function(next, data){
        tableRequest.KeySchema = tableDescription.Table.KeySchema;
        tableRequest.ProvisionedThroughput = tableDescription.Table.ProvisionedThroughput;
        this.isTableDeleted(table.name).then(next);

    }).then(function(next){
        console.log('table deleted');
        this.db.createTable(tableRequest, function(err, data){
            if (!err) {
                return next(data);
            }
            else {
                throw new Error(err);
            }
        });
    }).then(function(next, data){
        this.isTableActive(table.name).then(function(){
            d.resolve(true);
        });
    });
    return d.promise;
};

Model.prototype.isTableDeleted = function(tableName){
    var d = when.defer(),
        self = this;
    this.db.describeTable({
        'TableName': tableName
    }, function(err, data){
        if (data === undefined) {
            return d.resolve(true);
        }
        else {
            setTimeout(function(){
                self.isTableDeleted(tableName).then(function(_){
                    d.resolve(_);
                });
            }, 5000);
        }
    });
    return d.promise;
};

Model.prototype.isTableActive = function(tableName){
    var d = when.defer(),
        self = this;
    this.db.describeTable({
        'TableName': tableName
    }, function(err, data){
        if (data.Table.TableStatus === 'ACTIVE') {
            return d.resolve(true);
        }
        else {
            setTimeout(function(){
                self.isTableActive(tableName).then(function(_){
                    d.resolve(_);
                });
            }, 5000);
        }
    });
    return d.promise;
};

Model.prototype.batchWriteItem = function(alias){
    // this.db.batchWriteItem(request, function(err, data){
};

module.exports = Model;
