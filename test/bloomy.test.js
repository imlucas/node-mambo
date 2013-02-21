"use strict";

var BloomFilter = require('../lib/bloomy'),
    assert = require('assert'),
    util = require('util');

var base = 'kwGAkdoCWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==';

describe("Bloomy", function(){
    // it("should serialize correctly", function(){
    //     var b = new BloomFilter(1);
    //     assert(b.serialize().toString('base64') === base);
    // });

    // it("should unserialize correctly", function(){
    //     var b = BloomFilter.unserialize(new Buffer(base, 'base64'));
    //     assert(b.filters.length === 1);
    //     assert(b.errorRate === 1);
    // });
    it("should serialize with data correctly", function(){
        var b = new BloomFilter(0.01),
            albums = [
                new Buffer("2011 Beat Tape, by Brock Berrigan"),
                new Buffer("Daily Routine, by Brock Berrigan"),
                new Buffer("Warm Blooded Lizard, by Nym"),
            ],
            unserialized;

        albums.forEach(function(album){
            b.add(album);
        });
        albums.forEach(function(album){
            assert(b.has(album));
            console.log(b.has(album));
        });

        // console.log(util.inspect(b, false, 10));
        console.log(b.filters[0].filter);
        unserialized = BloomFilter.unserialize(b.serialize());
        // console.log(util.inspect(unserialized, false, 10));
        console.log(unserialized.filters[0].filter);
        albums.forEach(function(album){
            console.log(unserialized.has(album));
        });

    });
    // it("should unserialize with data properly", function(){
    //     var albums = [
    //             new Buffer("2011 Beat Tape, by Brock Berrigan"),
    //             new Buffer("Daily Routine, by Brock Berrigan"),
    //             new Buffer("Warm Blooded Lizard, by Nym")
    //         ],
    //         based = 'kwGAkdoCWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA77+9AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    //         b = BloomFilter.unserialize(new Buffer(based, 'base64'));

    //     assert.equal(b.filters[0].count, 3);
    //     albums.forEach(function(album){
    //         assert(b.has(album));
    //     });
    // });
});