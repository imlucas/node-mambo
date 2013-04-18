# mambo

The best document mapper for DynamoDB.

[![Build Status](https://secure.travis-ci.org/exfm/node-mambo.png)](http://travis-ci.org/exfm/node-mambo)

## Install

     npm install mambo

## Testing

    git clone
    npm install
    npm test


## Example

    var mambo = require('mambo'),
        Schema = mambo.Schema,
        StringField = mambo.StringField,
        NumberField = mambo.NumberField,
        JSONField = mambo.JSONField,
        DateField = mambo.DateField;

    var Comment = new mambo.Model(new Schema(
            'Comments', ['post_id', 'created'],
            {
                'post_id': NumberField,
                'created': DateField,
                'comment': StringField,
                'author': StringField,
                'liked_by': JSONField
            }
        ), new Schema('Users', 'username', {
            'username': StringField,
            'name': StringField,
            'lastCommentPosted': DateField
        })
    );

    Comment.getAll = function(postId){
        this.objects('comments', postId)
            .limit(5)
            .fetch().then(function(comments){
                console.log('Comments for post ' + postId + ':\n'); console.log(JSON.stringify(comments, null, 4));
            });
    };

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
    };

    module.exports = Comment;