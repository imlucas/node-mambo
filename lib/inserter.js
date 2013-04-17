"use strict";

var Q = require('q'),
    _ = require('underscore');

function Inserter(model, alias){
    this.model = model;
    this.schema = this.model.schema(alias);
    this.alias = alias;

    this.expectations = {};
    this.ops = {};
}

Inserter.prototype.shouldExist = function(name){
    return this.expect(name, true);
};

Inserter.prototype.shouldNotExist = function(name){
    return this.expect(name, false);
};

Inserter.prototype.shouldEqual = function(name, value){
    return this.expect(name, true, value);
};

Inserter.prototype.expect = function(name, exists, value){
    var field = this.schema.field(name);

    this.expectations[name] = {};
    if(value !== undefined && !exists){
        exists = true;
    }
    this.expectations[name].Exists = exists;
    if(value !== undefined && exists === true){
        this.expectations[name].Value = field.export(value);
    }
    return this;
};

Inserter.prototype.from = function(obj){
    for(var key in obj){
        if(this.schema.hasField(key)){
            this.ops[key] = obj[key];
        }
    }
    return this;
};

Inserter.prototype.set = function(opts){
    for(var key in opts){
        this.ops[key] = opts[key];
    }
    return this;
};

Inserter.prototype.commit = function(){
    var d = Q.defer();

    this.model.put(this.alias, this.ops,
        this.expectations, false).then(
        function(data){
            d.resolve(data);
        },
        function(err){
            d.reject(err);
        }
    );
    return d.promise;
};

Inserter.prototype.insert = function(alias){
    // Upgrade to batch inserter
    // @todo (lucas) This should use batch.Batch.
    if(Object.keys(this.expectations).length > 0){
        throw new Error("Can't upgrade to a batch with expectations.");
    }

    var i = new BatchInserter(this.model);
    i.fromInserter(this);
    i.insert(alias);
    return i;
};

function BatchInserter(model){
    this.model = model;
    this.lastAlias = null;
    this.puts = {};
    this.numOps = 0;
    this.chunkSize = 25;
}

BatchInserter.prototype.chunk = function(s){
    this.chunkSize = s;
    return this;
};

BatchInserter.prototype.insert = function(alias){
    this.lastAlias = alias;
    if(!this.puts.hasOwnProperty(alias)){
        this.puts[alias] = [];
    }
    return this;
};

BatchInserter.prototype.fromInserter = function(inserter){
    this.insert(inserter.alias);
    this.set(inserter.ops);
    return this;
};

BatchInserter.prototype.set = function(ops){
    this.puts[this.lastAlias].push(ops);
    this.numOps++;
    return this;
};

BatchInserter.prototype.commit = function(){
    var d = Q.defer();
    if(this.numOps <= this.chunkSize){
        this.model.batchWrite(this.puts, {}).then(d.resolve, d.reject);
    }
    else {
        // Split the work into chunks of 25.
        var chunks = [],
            index = 0,
            item,
            chunkSizes = [0];

        Object.keys(this.puts).forEach(function(alias){
            while(item = this.puts[alias].shift()){
                if(chunkSizes[index] === this.chunkSize){
                    index++;
                    chunkSizes[index] = 0;
                }
                var chunk = chunks[index];

                if(!chunk){
                    chunks[index] = {};
                }
                if(!chunks[index][alias]){
                    chunks[index][alias] = [];
                }
                chunks[index][alias].push(item);
                chunkSizes[index]++;
            }
        }.bind(this));

        Q.all(chunks.map(function(chunk){
            var p = Q.defer();
            this.model.batchWrite(chunk, {}).then(p.resolve, p.reject);
            return p.promise;
        }.bind(this)), function(results){
            var result = {
                'success': {},
                'unprocessed': {}
            };
            results.forEach(function(r){
                for(var alias in r.success){
                    if(!result.success.hasOwnProperty(alias)){
                        result.success[alias] = 0;
                    }
                    result.success[alias] += r.success[alias];
                }

                for(var tableName in r.unprocessed){
                    if(!result.unprocessed.hasOwnProperty(tableName)){
                        result.unprocessed[tableName] = [];
                    }
                    r.unprocessed[tableName].forEach(function(i){
                        result.unprocessed[tableName].push(i);
                    }.bind(this));

                }

            }.bind(this));

            d.resolve(result);
        });
    }
    return d.promise;
};

module.exports = Inserter;