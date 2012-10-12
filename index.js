"use strict";

var dynamo = require("dynamo"),
    when = require("when"),
    sequence = require("sequence"),
    winston = require("winston");

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

Model.prototype.ensureTable = function(tableData){
    log.debug("ensuring table: " + tableData.tableName);
    var d = when.defer();
    sequence(this).then(function(next){
        // Delay to avoid dynamo ThrottlingException
        setTimeout(next, 100);
    }).then(function(next){
        // Does the table already exist in DynamoDB?
        log.debug("Does " + tableData.tableName + " already exist in DynamoDB?");
        this.db.describeTable({'TableName': tableData.tableName}, next);
    }).then(function(next, err, description){
        log.debug("Got error or description for: " + tableData.tableName);
        if(err){
            log.debug("Got err for: " + tableData.tableName + ": " + err);
            return next(err);
        }
        else if(description.CreationDateTime !== 0){
            // Table already exists
            log.debug(tableData.tableName + " already exists");
            return d.resolve(true);
        }
        else {
            log.error("Problem getting " + tableData.tableName + " description: " + description);
            return d.resolve(false);
        }
    }).then(function(next, err){
        log.debug("Got err (which is a good thing) for: " + tableData.tableName);
        if(err.name.indexOf("ResourceNotFound") !== -1){
            log.debug(tableData.tableName + " doesn't exist yet, so create it.");
            // Table doesn't exist yet, so create it.
            return next();
        }
        log.error("Error getting " + tableData.tableName + " description: " + err);
        return d.resolve(false);
    }).then(function(next){
        // Delay to avoid dynamo ThrottlingException
        setTimeout(next, 100);
    }).then(function(next){
        // Create the keySchema data
        log.debug("Create the keySchema data");
        var keySchema = {
            'HashKeyElement': {
                'AttributeName': tableData.hashName,
                'AttributeType': tableData.hashType
            }
        };
        if(tableData.rangeName){
            keySchema.RangeKeyElement = {
                'AttributeName': tableData.rangeName,
                'AttributeType': tableData.rangeType
            };
        }
        log.debug("keySchema: " + JSON.stringify(keySchema));
        next(keySchema);
    }).then(function(next, keySchema){
        // Create the table in DynamoDB
        log.debug("Create " + tableData.tableName + " in DynamoDB");
        this.db.createTable({
            'TableName': tableData.tableName,
            'ProvisionedThroughput': {
                'ReadCapacityUnits': tableData.read,
                'WriteCapacityUnits': tableData.write
            },
            'KeySchema': keySchema
        }, next);
    }).then(function(next, err, table){
        log.debug("Finished creating " + tableData.tableName);
        if(!d.rejectIfError(err)){
            log.info(tableData.tableName + " created in DynamoDB: " + JSON.stringify(table));
            return d.resolve(true);
        }
        log.debug("Something went wrong with : " + tableData.tableName + ": " + err);
    });
    return d.promise;
};


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

        this.tableData.forEach(function(tableData){
            log.debug("table data: " + JSON.stringify(tableData));
            var tableName = (this.prefix || "") + tableData.table;
            tableData.tableName = tableName;

            this.ensureTable(tableData).then(function(success){
                if(success){
                    this.tables[tableData.alias] = this.db.get(tableData.tableName);

                    // @todo is the following necessary?
                    this.tables[tableData.alias].name = this.tables[tableData.alias].TableName;
                    this.tables[tableData.alias].girth = {
                        'read': tableData.read,
                        'write': tableData.write
                    };

                    // Parse table hash and range names and types defined in package.json
                    var localSchema = {},
                        typeMap = {
                            'N': Number,
                            'Number': Number,
                            'NS': [Number],
                            'NumberSet': [Number],
                            'S': String,
                            'String': String,
                            'SS': [String],
                            'StringSet': [String]
                        };
                    localSchema[tableData.hashName] = typeMap[tableData.hashType];
                    if (tableData.rangeName){
                        localSchema[tableData.rangeName] = typeMap[tableData.rangeType];
                    }

                    this.tables[tableData.alias].schema = localSchema;

                    this.girths[tableData.alias] = {
                        'read': tableData.read,
                        'write': tableData.write
                    };
                }
                else {
                    log.error(tableData.tableName + " was not created.");
                }
            }.bind(this));
        }.bind(this));

    } else {
        // Connect to Magneto
        log.info("Connecting to Magneto");
        this.client = dynamo.createClient();
        this.client.useSession = false;

        this.db = this.client.get(this.region || "us-east-1");

        this.db.host = "localhost";
        this.db.port = 8081;

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
    }

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
        if(!d.rejectIfError(err)){
            log.debug("!d.rejectIfError(err)");
            return d.resolve(data);
        }
        return d.resolve(err);
    });
    return d.promise;
};

module.exports = Model;
