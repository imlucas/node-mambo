# Mambo

Mambo is a document mapper for Amazon's [DynamoDB](http://aws.amazon.com/dynamodb/), a fully managed document database.  Dynamo exposes a very spartan API because it is designed for consistent performance and very high scalability.  Mambo provides

 * casting: Dynamo offers (number, string and binary fields), mambo provides higher level javascript types (Object, Array, Boolean, Date and many others).
 * fluent api: Chainable instances for Query's, Scan's, Update's and Insert's.

## Tutorial
### Defining Schemas

Mambo allows you to define schemata for table items to make dynamo friendlier to use. casting Dates, strings, and numbers correctly.  Applying default / nullable values (dynamo doesnt really allow nulls).

To define a schema:

    var mambo = require('mambo'),
        Schema = mambo.Schema,
        StringField = mambo.StringField,
        DateField = mambo.DateField;

    var PageSchema = new Schema('Page', 'page', 'id', {
        'id': StringField,
        'title': StringField,
        'content': StringField,
        'hits': NumberField,
        'update_count': NumberField,
        'date_modified': DateField
    });

    var model = new mambo.Model(PageSchema);

The arguments for the schema constructor are:

 * Base Table Name - Allows global prefixing to easily support switching environments.
 * Table Alias - Because the table name may change with prefixing or may need to be longer.  The alias is used to refer to a specific schema in all commands.
 * Dynamo Schema Keys - String of the key name for a hash table.  Array for a hash range table, hash key and range key respectively.
 * Spec - Key to field type mapping.  Used for ensuring data is typed correctly going in to dynamo and cast to more complex types coming out.  More on that below.


### Fields

 Dynamo has a very miminal set of built in field types:

 * `StringField`
 * `StringSetField`
 * `NumberField`
 * `NumberSetField`
 * `BinaryField`
 * `BinarySetField`

Mambo provides a few others:

 * `DateField` - Date objects going in and out.  Stored as NumberField.
 * `JSONField` - Arrays or Objects.  Stored as StringField.
 * `BooleanField` - Stored as NumberField.

### Connecting to Dynamo

Models expose a connect method:

    model.connect(key, secret, prefix, region)

As you would expect, to connect to dynamo you'll need to provide an AWS key and secret.  Connecting is synchronous and emits a `connect` event for extensibility.

Prefix is a table name prefix for easily running multiple environments with the same table names.

Region's are not fully supported yet.

Currently, each model connects indepently, but it is probably a good idea to add a top level `mambo.connect` method like mongoose.

### Inserting Documents

`Model.insert` provides a nice wrapper for calling `PutItem`:

    var info = {
        'id': 'about',
        'title': 'About Page',
        'content': 'TBD',
        'date_modified': new Date()
    };
    model.insert('page')
        .set(info)
        .commit(callback);

You can also easily apply logic for conditional puts:

    model.insert('page')
        .set(info)
        .shouldNotExist('id')  // All 3 expectations the same.
        .expect('id', false)
        .expect('id', false, null)
        .commit(callback);

### Updating Documents

Mambo exposes an [`UpdateQuery`](https://github.com/exfm/node-mambo/blob/master/lib/update-query.js) instance to make updates fluent and batch-able.

    model.update('page', 'about')
        .set({
            'content': 'These are some things about us.',
            'date_modified': new Date()
        })
        .inc('update_count', 1)
        .returnNone()
        .commit(callback);

`UpdateQuery` exposes

 * `returnNone`, `returnAllOld`, `returnAllNew`, `returnUpdatedOld`, `returnUpdatedNew`: Control the ReturnValues for an update
 * `inc`, `dec`, `push`, `set`: Atomic update operators
 * `expect`: Specify conditional puts, just like `insert`

### Querying Documents

Dynamo exposes three read operators: `Get`, `Query` and `Scan`.

#### Get

[`Get`](https://github.com/exfm/node-mambo/blob/master/index.js#L179) is for fetching a single document by hash key or hash AND range key.

    model.get('page', 'about', function(err, doc){
        if(err){
            return console.error('Error fetching page: ' + err);
        }
        console.log('Got page: ' + JSON.stringify(doc));
    });

You can also specify attributes to fetch and whether the read should be consistent.

    model.get('page', 'about', undefined, ['id'], true, function(err, doc){
        if(err){
            return console.error('Error fetching page: ' + err);
        }
        console.log('Got consistent doc that only has id: ' + JSON.stringify(doc));
    });

#### Query

[`Query`](https://github.com/exfm/node-mambo/blob/master/index.js#L606) is one level up from get.  It's primary use is for fetching linked documents.  Say we wanted to add logs for page views.  Our hash key will be the page id and we'll choose the current time in ms for our range key.

    var logSchema = new Schema('PageLog', 'log', ['id', 'timestamp'], {
        'id': StringField,
        'timestamp': DateField,
        'ip': StringField
    });

    model.insert('log').set({
        'id': 'about',
        'timestamp': new Date(),
        'ip': '10.0.0.0'
    }).commit();

    model.insert('log').set({
        'id': 'about',
        'timestamp': new Date(),
        'ip': '10.0.0.1'
    }).commit();


No if we wanted to get a list of all ip's that have viewed the about page:

    model.query('log', 'about', function(err, docs){
        console.log('ips that viewed about: ' + docs.map(function(doc){
            return doc.ip;
        }));
    });

#### Scan

[`Scan`](https://github.com/exfm/node-mambo/blob/master/index.js#L693) is a full table scan.  Scan can be extremely useful, but can be quite literally very expensive.  Mambo provides a [`Scanner`](https://github.com/exfm/node-mambo/blob/master/lib/scan.js) instance to make every the harry-est of queries very simple.  You have the following operators available for a scan:

 * ==
 * !=
 * <
 * <=
 * >
 * >=
 * NOT_NULL
 * NULL
 * CONTAINS
 * NOT_CONTAINS
 * BEGINS_WITH
 * IN
 * BETWEEN

Say we wanted to change our log's to include a username and state and we wanted to view the most visited pages in NY today. (yes, this is probably better done as an elastic map reduce job, but play along smartypants.)

    var logSchema = new Schema('PageLog', 'log', ['id', 'timestamp'], {
        'id': StringField,
        'timestamp': DateField,
        'ip': StringField,
        'day': StringField,
        'state': StringField,
        'username': StringField
    });

And let's insert some fresh data.

    var howManyEntries = 100,
        now =  new Date(),
        startTime = now.getTime(),
        today = [now.getYear(), now.getMonth(), now.getDay()].join(),
        pages = ['about', 'home', 'user/lucas', 'user/dan', 'user/jm', 'user/majman'],
        usernames = ['lucas', 'dan', 'jm', 'majman'],
        ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']
        page, username, ip, i;

    for(i = 0; i < howManyEntries; i++){
        page = pages[Math.floor(Math.random() * pages.length)];
        ip = ip[Math.floor(Math.random() * ip.length)];
        username = usernames[Math.floor(Math.random() * usernames.length)];
        model.insert('log').set({
            'id': page,
            'timestamp': startTime + 1,
            'ip': ip,
            'day': today,
            'username': username,
            'state': 'NY'
        }).commit();
    }

So now we're inserted 100 new items, with 2 from our previous deploy.  Let's run a scan to get only those pageviews that were for user pages and the visitor was in NY.

    var now = new Date(),
        today = [now.getYear(), now.getMonth(), now.getDay()].join(),
        pageToUsers = {};

    model.scan('log')
        .where('state', '==', 'NY')
        .where('state', '!=', null)  // Translates into state NOT_NULL
        .where('id', 'BEGINS_WITH', 'user/')
        .where('day', '==', today)
        .fields(['id', 'username'])
        .fetch(function(docs){
            docs.forEach(function(doc){
                if(!pageToUsers.hasOwnProperty(doc.id)){
                    pageToUsers[doc.id] = [];
                }
                pageToUsers[doc.id].push(doc.username);
            });
        });

### Batch

In our example above, we inserted 100 log items, which resulted in 100 API calls to dynamo.  But we can make things run even faster by using batch writes.  As you would expect, batch writes allow multiple writes per API call, up to 25 items.  This is really nice, but feels kind of clunky manually batching things up.  Instead, mambo splits your requests into batches of 25 automatically; looks like a single request, returns a single response.

    var batch = model.batch();

    for(i = 0; i < howManyEntries; i++){
        page = pages[Math.floor(Math.random() * pages.length)];
        ip = ip[Math.floor(Math.random() * ip.length)];
        username = usernames[Math.floor(Math.random() * usernames.length)];
        batch.insert('log', {
            'id': page,
            'timestamp': startTime + 1,
            'ip': ip,
            'day': today,
            'username': username,
            'state': 'NY'
        });
    }
    batch.commit(callback);

@todo Batch Get

@todo Batch Get and Write Multi Table

### Testing

Testing your applications that use mambo is made extremely simple with [magneto](https://github.com/exfm/node-magneto), an in memory, mock dynamodb.
Just specify `MAMBO_BACKEND=magneto` as an environment variable and mambo will use magneto's rest api instead of dynamo.

Mambo also provides helpers for your tests. For example, with mocha

    describe('my tests', function(){
        before(function(done){
            model.connect(null, null, 'Testing');
            model.createAll(function(){
                console.log('Tables created.  Test away.');
                done();
            });
        });
        ... Your tests

Mambo also provides a shorthand for all of this.

    var mambo = require('mambo');
    mambo.use(mambo.testing());
    describe('my tests, function(){
        before(mambo.testing.before);
        after(mambo.testing.after);
        afterEach(mambo.testing.afterEach);
    }):

### Events

Mambo relays all events from the [plata](https://github.com/exfm/node-plata) "driver" that you can do really interesting things with.
The emitted events are `retry`, `successful retry`, and `stat`.

    model.on('retry', function(err, action, data){
        console.log('Mambo will retry the request `'+action+'` with data `'+data+'` because `'+err.message+'`');
        console.log('At this point, you could make a call to "autoscale" your throughput for `'+date.TableName+'`');
    });

    model.on('retry successful', function(err, action, data){
        console.log('The retry of `'+action+'` with data `'+data+'` because `'+err.message+'` was successful.');
    });

    model.on('stat', function(stat, action, data){
        console.log('The action `'+action+'` with data `'+data+'` used `'+stat.consumed+'` capacity units.');
    });

These are extremely useful for debugging and can be used for really interesting tools like [autoscaling your table throughput](https://github.com/exfm/node-dynascale).

### Custom Fields

@todo

## Other Links

 * [Changelog](https://github.com/exfm/node-mambo/blob/master/CHANGELOG.md)
 * [Issues](https://github.com/exfm/node-mambo/issues)

## License

MIT
