"use strict";

var when = require('when'),
    Model = require('../'),
    StringField = Model.StringField,
    DateField = Model.DateField,
    Schema = Model.Schema;

// var mutex = new Mutex('some-name', 10);
// mutex.lock().then(function(){
//     // Do some stuff
//     mutex.unlock().then(function(){
//         console.log('Unlocked.  Lock away.');
//     });
// }, function(err){
//     console.error('Couldn\'t accquire lock');
// });
function Mutex(id, ttl){
    this.id = id;
    this.ttl = ttl;
    this.model = Mutex.model;
    if(!this.model.connected){
        this.model.connect();
    }
}

Mutex.model = new Model(new Schema('Locks', 'lock', 'id', {
    'id': StringField,
    'created': DateField
}));

Mutex.prototype.lock = function(){
    var self = this,
        d = when.defer();

    this.pruneExpired().then(function(){
        self.model.insert('lock')
            .set({
                'id': self.id,
                'created': new Date()
            })
            .expect('id', false)
            .commit()
            .then(function(){
                d.resolve(self);
            }, function(err){
                d.reject(err);
            });
    }, function(err){
        d.reject(err);
    });
    return d.promise;
};

Mutex.prototype.unlock = function(){
    var self = this,
        d = when.defer();

    self.model.delete('lock', self.id).then(function(){
        d.resolve(self);
    }, function(err){
        d.reject(err);
    });
    return d.promise;
};

Mutex.prototype.pruneExpired = function(){
    var self = this,
        d = when.defer();
    self.model.get('lock', self.id, undefined, undefined, true).then(function(item){
        if(item){
            if(item.created.getTime() < (Date.now() - (self.ttl * 1000))){
                return self.unlock().then(function(){
                    d.resolve(self);
                }, function(err){
                    d.reject(err);
                });
            }
            else{
                return d.reject(new Error('Lock expires at ' + new Date(
                    item.created.getTime() + (self.ttl * 1000))));
            }
        }
        return d.resolve(self);
    }, function(err){
        d.reject(err);
    });
    return d.promise;
};

module.exports = Mutex;