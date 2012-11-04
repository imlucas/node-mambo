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

Batch.prototype.update = function(alias, hash, range){
    throw new Error('Can\'t do updates in a batch!');
};

Batch.prototype.set = function(ops){
    this.puts[this.lastAlias].push(ops);
    this.numOps++;
    return this;
};

Batch.prototype.remove = function(alias, hash, range){
    this.lastAlias = alias;
    if(this.model.schema(alias).range !== undefined && range === undefined){
        this.requiredScans.push({
            'alias': alias,
            'hash': hash
        });
    }
    else{
        this.addDelete(alias, hash, range);
    }
    return this;
};
Batch.prototype.addDelete = function(alias, hash, range){
    if(!this.deletes.hasOwnProperty(alias)){
        this.deletes[alias] = [];
    }
     var k = {},
        schema = this.model.schema(alias);

    k[schema.hash] = hash;
    if(range !== undefined){
        k[schema.range] = range;
    }
    this.deletes[alias].push(k);
    this.numOps++;
    return k;
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
    if(this.requiredScans.length > 0){
        when.all(this.requiredScans.map(function(scan){
            var p = when.defer(),
                schema = this.model.schema(scan.alias);

            this.model.objects(scan.alias, scan.hash)
                .fields([schema.range])
                .fetch()
                .then(function(rows){
                    rows.forEach(function(row){
                        this.addDelete(scan.alias,
                            scan.hash, row[schema.range]);
                    }.bind(this));
                    p.resolve();
                }.bind(this));
            return p.promise;
        }.bind(this)), function(){
            this.processCommit(d);
        }.bind(this));
    }
    else{
        this.processCommit(d);
    }
    return d.promise;
};

Batch.prototype.processCommit = function(d){
    if(this.numOps <= this.chunkSize){
        this.model.batchWrite(this.puts, this.deletes).then(d.resolve, d.reject);
    }
    else {
        when.all(this.splitOpsIntoChunks().map(function(chunk){
            var p = when.defer();
            this.model.batchWrite(chunk, this.deletes).then(p.resolve, p.reject);
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
};

module.exports = Batch;