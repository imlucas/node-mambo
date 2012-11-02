"use strict";

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

Comment.getAll = function(postId, cb){
    this.objects('comments', postId)
        .limit(5)
        .fetch().then(function(comments){
            cb(null, comments);
        }, function(err){
            cb(err, null);
        });
};

Comment.post = function(postId, author, comment, cb){
    this.insert('comments',
        {
            'post_id': postId,
            'created': new Date(),
            'author': author,
            'comment': comment,
            'liked_by': []
        })
        .commit()
        .then(function(){
            cb(null);
        }, function(err){
            cb(err);
        }
    );
};

module.exports = Comment;