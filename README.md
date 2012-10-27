# mambo

Little wrapper for dynamo models


## Install

     npm install mambo

## Testing

    git clone
    npm install
    mocha


## Example

    var mambo = require('mambo'),
        Schema = mambo.Schema,
        StringField = mambo.StringField,
        NumberField = mambo.NumberField,
        JSONField = mambo.JSONField,
        DateField = mambo.DateField;

    var schema = new Schema({
            'post_id': NumberField,
            'created': DateField,
            'comment': StringField,
            'author': StringField,
            'liked_by': JSONField
        },
        {
            'hash': 'post_id',
            'range': 'created',
            'alias': 'comments',
            'name': 'Comments'
        }
    );

    mambo.tablePrefix('TestEnv');

    var Comment = new mambo.Model(schema);
    Comment.getAll = function(postId){
        this.objects('comments', postId)
            .limit(5).
            .fetch().then(function(comments){
                console.log('Comments for post ' + postId + ':\n'); console.log(JSON.stringify(comments, null, 4));
            });
    });

    Comment.post = function(postId, author, comment){
        this.insert('comments',
            {
                'post_id': postId,
                'created': new Date(),
                'author': author,
                'comment': comment,
                'liked_by': []
            })
            .commit().then(function(){
            console.log('Comment added!');
        });
    });

    module.exports = Comment;