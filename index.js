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
        this.tables[table.alias].girth = {
            'read': table.read,
            'write': table.write
        };

        // Parse table hash and range names and types defined in package.json
        schema[table.hashName] = typeMap[table.hashType];
        if (table.rangeName){
            schema[table.rangeName] = typeMap[table.rangeType];
        }

        this.tables[table.alias].schema = schema;

        this.girths[table.alias] = {
            'read': table.read,
            'write': table.write
        };
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

Model.prototype.get = function(alias, key, value){
    var d = when.defer(),
        query = {};

    query[key] = value;
    this.table(alias).get(query).fetch(function(err, data){
        if(err){
            return d.resolve(data);
        }
        return d.resolve(err);
    });
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


Model.prototype.toDynamo = function(tableName, obj){
    var dynamoObj = {'TableName': tableName, 'Item': {}};

    Object.keys(obj).map(function(attr){
        if(accept(obj[attr])){
            var attrType = this.attributeSchema[tableName][attr],
                value = obj[attr];

            if(value == true){
                value = 1;
            }
            if(value == false){
                value = 0;
            }
            if(attrType === "N"){
                value = value.toString();
            }
            if(attrType === "NS"){
                var newValue = [];
                value = value.forEach(function(item){
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

Model.prototype.fromDynamo = function(dynamoObj){
    var obj = {};

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

// @todo(jonathan) Make this work
// Model.prototype.update = function(tableName, newObj){
//     var d = when.defer(),
//         dynamoObj;

//     // @todo(jonathan) format newObj properly
//     // http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_UpdateItem.html

//     this.db.updateItem(
//         dynamoObj,
//         function(err, data){
//             if(!err){
//                 return d.resolve(data);
//             }
//             return d.resolve(err);
//         }
//     );
//     return d.promise;
// };

module.exports = Model;
