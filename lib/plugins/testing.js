"use strict";

var debug = require('plog')('mambo:testing'),
    mambo = require('../../'),
    instances = mambo.instances,
    async = require('async'),
    magneto = require('magneto');

module.exports = function(){
    magneto.server = magneto.server || null;
    process.env.MAMBO_BACKEND = 'magneto';
};

module.exports.recreateTable = function(instance, alias, done){
    instance.deleteTable(alias, function(){
        instance.createTable(alias, 5, 5, done);
    });
};

// Drop all tables for all instances and rebuild them.
module.exports.recreateAll = function(done){
    var tasks = [];
    instances.map(function(instance){
        Object.keys(instance.schemasByAlias).map(function(alias){
            tasks.push(function(callback){
                module.exports.recreateTable(instance, alias, callback);
            });
        });
    });
    async.parallel(tasks, done);
};

module.exports.dropAll = function(done){
    var tasks = [];
    instances.map(function(instance){
        Object.keys(instance.schemasByAlias).map(function(alias){
            tasks.push(function(callback){
                instance.deleteTable(alias, callback);
            });
        });
    });
    async.parallel(tasks, done);
};

module.exports.before = function(done){
    function onReady(){
        debug('recreating all tables for testing...');
        mambo.createAll(function(){
            if(done){
                return done();
            }
            return true;
        });
    }
    if(magneto.server){
        return onReady();
    }
    debug('starting magneto on port 8081...');
    magneto.server = magneto.listen(8081, function(){
        onReady();
    });
};

module.exports.afterEach = function(done){
    return module.exports.recreateAll(function(){
        if(done){
            return done();
        }
        return true;
    });
};

module.exports.after = function(done){
    debug('calling drop all');
    return module.exports.dropAll(function(){
        debug('all tables dropped');
        if(magneto.server){
            debug('stopping magneto');
            magneto.server.close();
            magneto.server = null;
        }
        if(done){
            return done();
        }
        return true;
    });
};