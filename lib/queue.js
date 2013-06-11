"use strict";
var async = require('async');

// var queue = new Queue('cloudsearch-user-jobs', 'id');
// queue.put({'id': 'jm', 'op': 'DELETE', 'ts': Date.now()});
// queue.put({'id': 'lucas', 'op': 'ADD', 'ts': Date.now()});
// setInterval(function(){
//     queue.get(1, function(err, res){
//         if(res.data[0].id === 'jm'){
//             return res.retry();
//         }
//         // Throw updates to cloud search
//         model.get('user', res.data.id).then(function(user){
//             aws.cloudSearch[res.data.ops](res.data.id, ts, user).commit();
//         });
//     });
// }, 1000);
function Queue(id, itemKey){
    this.id = id;
    this.itemKey = itemKey;
}

Queue.prototype.put = function(item, done){
    var itemId = this.id + '-' + item[this.itemKey];
    this.model.insert('queue').set({'id': itemId,'data': item}).commit(done);
};

Queue.prototype.get = function(n, done){
    var self = this;

    n = n || 1;

    this.model.scan('queue')
        .where('id', 'BEGINS_WITH', self.id + '-')
        .limit(n)
        .fetch(function(err, res){
            // Delete them.
            var ids = res.items.map(function(item){
                    return item.id;
                }),
                b = self.model.batch();

            ids.map(function(id){
                b.remove('queue', id);
            });
            b.commit(function(){
                done(null, new QueueResult(self, res.items));
            });
        });
};

function QueueResult(queue, items){
    this.queue = queue;
    this.items = items;
    this.data = items.map(function(item){
        return item.data;
    });
}

QueueResult.prototype.retry = function(done){
    // Put items back in the table.
    var self = this;
    async.parallel(this.items.map(function(item){
        return function(callback){
            self.queue.put(item.data, callback);
        };
    }), done);
};
module.exports = Queue;