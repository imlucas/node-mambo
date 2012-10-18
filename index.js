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
        log.info("Connecting to DynamoDB");
        this.client = dynamo.createClient({
            'accessKeyId': key,
            'secretAccessKey': secret
        });
        this.db = this.client.get(this.region || "us-east-1");
    } else {
        // Connect to Magneto
        // log.info("Connecting to Magneto");
        this.client = dynamo.createClient();
        this.client.useSession = false;
        this.db = this.client.get(this.region || "us-east-1");
        this.db.host = "localhost";
        this.db.port = process.env.MAGNETO_PORT || 8081;
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
            tableName = (this.prefix || "") + table.table;

        this.tables[table.alias] = this.db.get(tableName);
        this.tables[table.alias].name = this.tables[table.alias].TableName;

        this.tables[table.alias].read = table.read;
        this.tables[table.alias].write = table.write;
        // The girth attribute is redundant and I'm pretty sure we don't need it.
        this.tables[table.alias].girth = {
            'read': table.read,
            'write': table.write
        };
        this.girths[table.alias] = this.tables[table.alias].girth;

        this.tables[table.alias].hashType = table.hashType;
        this.tables[table.alias].hashName = table.hashName;
        if(table.rangeName){
            this.tables[table.alias].rangeType = table.rangeType;
            this.tables[table.alias].rangeName = table.rangeName;
        }
        this.tables[table.alias].key = {'HashKeyElement': {}};
        this.tables[table.alias].key.HashKeyElement[table.hashType] = table.hashName;
        if(table.rangeName){
            this.tables[table.alias].key.HashKeyElement[table.rangeType] = table.rangeName;
        }

        // Parse table hash and range names and types defined in package.json
        // I believe this is redundant and unused as well.
        schema[table.hashName] = typeMap[table.hashType];
        if (table.rangeName){
            schema[table.rangeName] = typeMap[table.rangeType];
        }
        this.tables[table.alias].schema = schema;

        this.tables[table.alias].attributeSchema = this.attributeSchema[table.table];

        this.tablesByName[tableName] = this.tables[table.alias];

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
        console.log(err);
        console.log(data);
        if(!err){
            return d.resolve(this.fromDynamo(alias, data.Item));
        }
        return d.resolve(err);
    }.bind(this));

    return d.promise;
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
        response = [],
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
                    response.push(obj);
                }.bind(this));
            }.bind(this));
            return d.resolve(response);
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

function convertType(item){
    if(item == true || item == 'true'){return 1;}
    if(item == false || item == 'false'){return 0;}
}

function sortResults(values, results, field){
    var sortedResults;
    values.forEach(function(value){
        results.forEach(function(object){
            if (object[field] === value) {
                sortedResults.push(object);
            }
        });

    });
    return sortedResults;
}

Model.prototype.toDynamo = function(tableName, obj){
    var dynamoObj = {'TableName': tableName, 'Item': {}};

    Object.keys(obj).map(function(attr){
        if(accept(obj[attr])){
            var attrType = this.attributeSchema[tableName][attr],
                value = obj[attr],
                newValue;

            if(value === true){
                value = "1";
            }
            if(value === false){
                value = "0";
            }
            if(attrType === "N"){
                value = value.toString();
            }
            if(attrType === "NS"){
                newValue = [];
                value.forEach(function(item){
                    newValue.push(item.toString);
                });
                value = newValue;
            }
            dynamoObj.Item[attr] = {};
            dynamoObj.Item[attr][attrType] = value;
        }
    }.bind(this));
    return dynamoObj;
};

Model.prototype.fromDynamo = function(alias, dynamoObj){
    var obj = {};
    Object.keys(dynamoObj).map(function(attr){
        var attrType = this.table(alias).attributeSchema[attr],
            value = dynamoObj[attr][attrType],
            newValue;

        if(attrType === "N"){
            value = parseInt(value, 10);
        }
        if(attrType === "NS"){
            newValue = [];
            value.forEach(function(n){
                newValue.push(parseInt(n, 10));
            });
            value = newValue;
        }
        // @todo Shit! Booleans are indistinguishable from numbers with values of 0 and 1.

        obj[attr] = value;
    }.bind(this));
    return obj;
};

Model.prototype.put = function(tableName, obj){
    var d = when.defer();

    this.db.putItem(obj, function(err, data){
        if(err){
            console.log("Error: " + err);
            throw err;
        }
        else {
            console.log("Created song: " + obj.Item.id.N);
            return d.resolve(obj.Item.id.N);
        }
        return d.resolve(err);
    });
    return d.promise;
};

Model.prototype.update = function(req){

    // usage:
    // update({
    //  'alias': 'song',
    //  'hash': 'blah',
    //  'range': 'blahblah',
    //  'attributeUpdates': [{
    //      'attributeName': 'attribute_name'
    //      'newValue': 'new_value',
    //      'action': 'PUT'
    //    }]
    //  'expectedValues': [{
    //      'attributeName': 'attribute_name',
    //      'expectedValue': 'current_value',
    //      'exists': 'true' // defaults to true
    //    }],
    //  'returnValues':  'NONE'
    // })

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
        attrSchema = this.table(req.alias).attributeSchema;

    table = this.table(req.alias);
    // updateRequest[table.name] = {};

    updateRequest = {
        'TableName': table.name,
        'Key': {},
        'AttributeUpdates': {},
        'ReturnValues': req.returnValues || 'NONE'
    };

    // Add hash
    if (req.hash){
        hashKey[table.hashType] = req.hash.toString();
        updateRequest.Key.HashKeyElement = hashKey;
    }
    // Add range
    if(req.range){
        rangeKey[table.rangeType] = req.range.toString();
        updateRequest.Key.RangeKeyElement = rangeKey;
    }
    // Add attributeUpdates
    if(req.attributeUpdates){
        req.attributeUpdates.forEach(function(attr){
            attributeUpdate = {
                'Value': {},
                'Action': attr.action || 'PUT'
            };
            attributeUpdate.Value[attrSchema[attr.attributeName]] = convertType(attr.newValue);
            updateRequest.AttributeUpdates[attr.attributeName] = attributeUpdate;
        });
    }
    // Add expectedValues for conditional update
    if(req.expectedValues){
        updateRequest.Expected = {};
        req.expectedValues.forEach(function(attr){
            expectedAttribute = {
                'Value': {},
                'Exists': attr.exists || convertType('true')
            };
            expectedAttribute.Value[attrSchema[attr.attributeName]] = convertType(attr.expectedValue);
            updateRequest.Expected[attr.attributeName] = attributeUpdate;
        });
    }

    // Make the request
    this.db.updateItem(updateRequest, function(err, data){
        if(!err){
            // translate the response from dynamo format to exfm format
            // req.forEach(function(tableData){
            //     table = this.table(tableData.alias);

            //     var items = data.Responses[table.name].Items;
            //     items.forEach(function(dynamoObj){
            //         obj = this.fromDynamo(tableData.alias, dynamoObj);
            //         response.push(obj);
            //     }.bind(this));
            // }.bind(this));
            return d.resolve(data);
        }
        return d.resolve(err);
    }.bind(this));
    return d.promise;
};

module.exports = Model;
