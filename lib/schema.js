"use strict";



// var SongSchema = new Schema({
//     'id': NumberField('id', {'hash': true}),
//     'title': StringField,
//     'artist': StringField,
//     'album': StringField,
//     'recent_loves': JSONField,
//     'created': new DateTimeField('created', {'range': true})
// });

function Schema(decl){
    this.fields = {};
    for(var key in decl){
        var field = decl[key];
        if(typeof field === 'function'){
            field = new field(key);
        }
        this.fields[field.name] = field;
    }
}

Schema.prototype.import = function(row){
    for(var key in this.fields){
        if(row.hasOwnProperty(key)){
            row[key] = this.fields[key].import(row[key]);
        }
    }
    return row;
};


Schema.prototype.export = function(row){
    for(var key in this.fields){
        if(row.hasOwnProperty(key)){
            row[key] = this.fields[key].export(row[key]);
        }
    }
    return row;
};

module.exports = Schema;