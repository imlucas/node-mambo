#!/usr/bin/env node
"use strict";

var base = process.argv[2],
    cut = process.argv[3],
    child_process = require('child_process'),
    fs = require('fs'),
    cmd,
    knownUsers = [
        ['Lucas Hrabovsky', 'imlucas'],
        ['Jonathan Marmor', 'jonathanmarmor']
    ];

if(!base || ! cut){
    console.log('update-changelog <base tag> <new tag>');
    process.exit(1);
}

cmd = 'git log '+base+'...master --pretty=format:"%h: %s - %an, %ad"';
child_process.exec(cmd, function(err, stdout, stderr){
    var entry = "## " + cut + "\n\n";

    stdout.split('\n').forEach(function(line){
        knownUsers.forEach(function(d, index){
            line = line.replace(d[0], '[' + d[1] + '](https://github.com/'+d[1]+')');
        });
        entry += line + '\n';
    });
    entry += "\n";

    fs.readFile('./CHANGELOG.md', 'utf-8', function(err, data){
        data = data.toString();
        fs.writeFile('./CHANGELOG.md', entry + data, 'utf-8', function(){
            console.log(entry);
        });
    });
});