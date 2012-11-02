"use strict";

var Comment = require('./model'),
    express = require("express"),
    app = express();

app.configure(function(){
    app.use(app.router);
    app.use(app.bodyParser());
});

app.get('/comments/:post_id', function(req, res, next){
    Comment.getAll(req.param('post_id'), function(err, comments){
        res.send(comments);
    });
});

app.post('/comment/:post_id', function(req, res, next){
    Comment.post(req.param('post_id'), req.param('author'), req.param('comment'), function(err){
        if(err){
            return res.send("Error posting comment: " + err);
        }
        res.send("Comment posted");
    });
});

app.listen(7000, function(){
    console.log('Mambo example app listening on http://localhost:7000/');
});