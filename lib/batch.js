"use strict";

var when = require('when');

// Song.batch()
//     .insert('song', songObject)
//     .update('song', 2)
//         .set()
//     .remove()
//     .remove('edit', songId, timestamp)
//     .commit();

function Batch(model){
    this.model = model;
    this.lastAlias = null;
    this.puts = {};
    this.deletes = {};
    this.numOps = 0;
    this.chunkSize = 25;
    this.requiredScans = [];
}

Batch.prototype.insert = function(alias, ops){
    this.lastAlias = alias;
    if(!this.puts.hasOwnProperty(alias)){
        this.puts[alias] = [];
    }
    if(ops !== undefined){
        this.set(ops);
    }
    return this;
};

// @todo (lucas) Add support for under the hood scans.
Batch.prototype.update = function(alias, hash, range){
    this.lastAlias = alias;
    if(!this.puts.hasOwnProperty(alias)){
        this.puts[alias] = [];
    }
    return this;
};

Batch.prototype.set = function(ops){
    this.puts[this.lastAlias].push(ops);
    this.numOps++;
    return this;
};

// @todo (lucas) Add support for under the hood scans.
Batch.prototype.remove = function(alias, hash, range){
    this.lastAlias = alias;
    if(!this.deletes.hasOwnProperty(alias)){
        this.deletes[alias] = [];
    }
    return this;
};



// Splits all puts and deletes into chunked operations,
// since we can only make a maximum of 25 item calls per batch request.
// This way you don't have to worry about the max size stuff on the client
// side, but have it accessible if you need to put a large batch
// of large items via `Batch.prototype.chunk`.
Batch.prototype.splitOpsIntoChunks = function(){
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
    Object.keys(this.deletes).forEach(function(alias){
        while(item = this.deletes[alias].shift()){
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
    return chunks;
};

Batch.prototype.commit = function(){
    var d = when.defer();

    if(this.numOps <= this.chunkSize){
        this.model.batchWrite(this.puts, {}).then(d.resolve, d.reject);
    }
    else {
        when.all(this.splitOpsIntoChunks().map(function(chunk){
            var p = when.defer();
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

                Object.keys(r.unprocessed).forEach(function(tableName){
                    if(!result.unprocessed.hasOwnProperty(tableName)){
                        result.unprocessed[tableName] = [];
                    }

                    r.unprocessed[tableName].forEach(function(i){
                        result.unprocessed[tableName].push(i);
                    });
                }.bind(this));
            }.bind(this));

            d.resolve(result);
        });
    }
    return d.promise;
};

module.exports = Batch;