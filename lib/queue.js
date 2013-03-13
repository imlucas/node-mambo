"use strict";
var when = require('when');

// var queue = new Queue('cloudsearch-user-jobs', 'id');
// queue.put({'id': 'jm', 'op': 'DELETE', 'ts': Date.now()});
// queue.put({'id': 'lucas', 'op': 'ADD', 'ts': Date.now()});
// setInterval(function(){
//     queue.get(1).then(function(res){
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

Queue.prototype.put = function(item){
    var self = this,
        itemId = self.id + '-' + item[self.itemKey],
        d = when.defer();

    self.model.insert('queue')
        .set({
            'id': itemId,
            'data': item
        })
        .commit()
        .then(function(res){
            d.resolve();
        }, function(err){
            d.resolve();
        });
    return d.promise;
};

Queue.prototype.get = function(n){
    var d = when.defer(),
        self = this;

    n = n || 1;

    this.model.scan('queue')
        .where('id', 'BEGINS_WITH', self.id + '-')
        .limit(n)
        .fetch()
        .then(function(res){
            // Delete them.
            var ids = res.items.map(function(item){
                    return item.id;
                }),
                b = self.model.batch();

            ids.map(function(id){
                b.remove('queue', id);
            });
            b.commit().then(function(){
                d.resolve(new QueueResult(self, res.items));
            });
        });
    return d.promise;
};

function QueueResult(queue, items){
    this.queue = queue;
    this.items = items;
    this.data = items.map(function(item){
        return item.data;
    });
}

QueueResult.prototype.retry = function(){
    // Put items back in the table.
    var self = this,
        d = when.defer();
    when.all(this.items.map(function(item){
        return self.queue.put(item.data);
    }), function(res){
        d.resolve(res);
    });
    return d.promise;
};
module.exports = Queue;