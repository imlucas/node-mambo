## Defining Schemas

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

 ## Fields

 Dynamo has a very miminal set of built in field types:

  - StringField
  - StringSetField
  - NumberField
  - NumberSetField

Mambo provides a few others:

 - DateField - Date objects going in and out.  Stored as NumberField.
 - JSONField - Arrays or Objects.  Stored as StringField.
 - BooleanField - Stored as NumberField.

## Inserting Documents

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



