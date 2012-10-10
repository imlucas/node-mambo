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

Model.prototype.connect = function(key, secret, prefix, region){
    this.prefix = prefix;
    this.region = region;

    if(process.env.NODE_ENV === "production"){
        // Connect to DynamoDB
        log.info("Connecting to DynamoDB");
        log.silly("AWS key: " + key + " secret: " + secret);

        this.client = dynamo.createClient({
            'accessKeyId': key,
            'secretAccessKey': secret
        });

        log.silly("dynamo client: " + this.client);

        this.client.useSession = false;

        this.db = this.client.get(this.region || "us-east-1");

        log.silly("DB: " + this.db);

    } else {
        // Connect to Magneto
        console.log("Connecting to Magneto");
        this.client = dynamo.createClient();
        this.client.useSession = false;

        this.db = this.client.get(this.region || "us-east-1");

        this.db.host = "localhost";
        this.db.port = 8081;
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
    when.all(Object.keys(this.tables).map(this.ensureTable.bind(this)), d.resolve);
    return d.promise;
};

Model.prototype.ensureTable = function(alias){
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
        log.silly("mambo err: " + err);
        log.silly("mambo data: " + data);
        if(!d.rejectIfError(err)){
            d.resolve(data);
        }
    });
    return d.promise;
};

module.exports = Model;
