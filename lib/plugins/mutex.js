"use strict";

var Model = require('../'),
    StringField = Model.StringField,
    DateField = Model.DateField,
    Schema = Model.Schema;

// var mutex = new Mutex('some-name', 10);
// mutex.lock(function(err){
//     if(err){
//         return console.error('Couldn\'t accquire lock');
//     }
//     // Do some stuff
//     mutex.unlock(function(){
//         console.log('Unlocked.  Lock away.');
//     });
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

Mutex.prototype.lock = function(done){
    var self = this;

    this.pruneExpired(function(err){
        if(err){
            return done(err);
        }
        self.model.insert('lock')
            .set({
                'id': self.id,
                'created': new Date()
            })
            .expect('id', false)
            .commit(done);
    });
};

Mutex.prototype.unlock = function(done){
    this.model.delete('lock', this.id, done);
};

Mutex.prototype.pruneExpired = function(done){
    var self = this;

    self.model.get('lock', self.id, undefined, undefined, true, function(err, item){
        if(err){
            return done(err);
        }

        if(item){
            if(item.created.getTime() < (Date.now() - (self.ttl * 1000))){
                return self.unlock(done);
            }
            else{
                return done(new Error('Lock expires at ' + new Date(
                    item.created.getTime() + (self.ttl * 1000))));
            }
        }
        done(null, self);
    });
};

module.exports = Mutex;