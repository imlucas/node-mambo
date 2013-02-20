# Mambo

Mambo is a document mapper for Amazon's [DynamoDB](http://aws.amazon.com/dynamodb/), a fully managed document database.  Dynamo exposes a very spartan API because it is designed for consistent performance and very high scalability.  Mambo provides casting of the primitive types Dynamo offers (number, string and binary fields) into higher level javascript types (JSON, Boolean, Date and many other fields).  

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

  - `StringField`
  - `StringSetField`
  - `NumberField`
  - `NumberSetField`
  - `BinaryField`
  - `BinarySetField`

Mambo provides a few others:

 - `DateField` - Date objects going in and out.  Stored as NumberField.
 - `JSONField` - Arrays or Objects.  Stored as StringField.
 - `BooleanField` - Stored as NumberField.

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
        .commit()
        .then(successHandler, errorHandler);

You can also easily apply logic for conditional puts:

    model.insert('page')
        .set(info)
        .shouldNotExist('id')  // All 3 expectations the same.
        .expect('id', false)
        .expect('id', false, null)
        .commit()
        .then(successHandler, errorHandler);

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

## Other Links

 * [Changelog](https://github.com/exfm/node-mambo/blob/master/CHANGELOG.md)
 * [Issues](https://github.com/exfm/node-mambo/issues)

## License

MIT
