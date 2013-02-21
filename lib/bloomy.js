"use strict";

// Adapated from https://github.com/wiedi/node-bloem

var msgpack = require('msgpack'),
    VNF = require('fnv').FNV,
    util = require('util');

function BitBuffer(number) {
    this.buffer = new Buffer(Math.ceil(number / 8));
    this.buffer.fill(0);
}

BitBuffer.prototype.set = function(index, bool){
    if(bool) {
        this.buffer[index >> 3] |= 1 << (index % 8);
    } else {
        this.buffer[index >> 3] &= ~(1 << (index % 8));
    }
};

BitBuffer.prototype.get = function(index) {
    return (this.buffer[index >> 3] & (1 << (index % 8))) !== 0;
};

function calculateSize(capacity, error_rate) {
    var log2sq = 0.480453;  /* Math.pow(Math.log(2), 2) */
    return Math.ceil(capacity * Math.log(error_rate) / -log2sq);
}

function calculateSlices(size, capacity) {
    return size / capacity * Math.log(2);
}

function calulateHashes(key, size, slices) {
    /* See:
     * "Less Hashing, Same Performance: Building a Better Bloom Filter"
     * 2005, Adam Kirsch, Michael Mitzenmacher
     * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.72.2442
     */
    function fnv(seed, data) {
        var h = new VNF();
        h.update(seed);
        h.update(data);
        return h.value();
    }
    var h1 = fnv(new Buffer("S"), key);
    var h2 = fnv(new Buffer("W"), key);
    var hashes = [];
    for(var i = 0; i < slices; i++) {
        hashes.push((h1 + i * h2) % size);
    }
    return hashes;
}

function Bloom(size, slices, buffer) {
    this.size   = size;
    this.slices = slices;
    this.bitfield = new BitBuffer(size);
    if(buffer){
        this.bitfield.buffer = buffer;
    }
}

Bloom.prototype.add = function(key) {
    var hashes = calulateHashes(key, this.size, this.slices);
    for(var i = 0; i < hashes.length; i++) {
        this.bitfield.set(hashes[i], true);
    }
};

Bloom.prototype.has = function(key) {
    var hashes = calulateHashes(key, this.size, this.slices);
    for(var i = 0; i < hashes.length; i++) {
        if(!this.bitfield.get(hashes[i])){
            return false;
        }
    }
    return true;
};

Bloom.prototype.serialize = function(){
    return msgpack.pack([this.size, this.slices, this.bitfield.buffer.toString('binary')]);
};

Bloom.unserialize = function(buf){
    var data = msgpack.unpack(buf),
        b = new Bloom(data[0], data[1], new Buffer(data[2], 'binary'));
    return b;
};

function SafeBloom(capacity, errorRate, buffer, count) {
    var size   = calculateSize(capacity, errorRate),
        slices = calculateSlices(size, capacity);

    this.capacity   = capacity;
    this.errorRate = errorRate;
    this.count  = count || 0;
    this.filter = new Bloom(size, slices);
    if(buffer){
        this.filter.bitfield.buffer = buffer;
    }
}

SafeBloom.prototype.add = function(key){
    if(this.count >= this.capacity){
        return false;
    }
    this.filter.add(key);
    this.count++;
    return true;
};

SafeBloom.prototype.has = function(key) {
    return this.filter.has(key);
};

SafeBloom.prototype.serialize = function(){
    return msgpack.pack([this.capacity, this.errorRate,
        this.filter.bitfield.buffer.toString('binary')]);
};

SafeBloom.unserialize = function(buf){
    var data = msgpack.unpack(buf),
        b = new SafeBloom(data[0], data[1], new Buffer(data[2], 'binary'));
    return b;
};

function ScalingBloom(errorRate, options, bufs, counts){
    this.options = options = options || {};
    this.errorRate = errorRate;
    this.ratio = options.ratio || 0.9;
    this.scaling = options.scaling || 2;
    this.initialCapacity = options.initialCapacity || 10;
    this.filters = [];
    if(bufs){
        this.filters.push(new SafeBloom(this.initialCapacity,
            errorRate * (1 - this.ratio), bufs.shift(),
            counts.shift()));
        bufs.forEach(function(buf, index){
            var f = this.filters.slice(-1)[0];
            this.filters.push(
                new SafeBloom(f.capacity * this.scaling,
                    f.errorRate * this.ratio, buf, counts[index])
            );
        }.bind(this));
    }
    else{
        this.filters.push(new SafeBloom(this.initialCapacity,
            errorRate * (1 - this.ratio)));
    }
}

ScalingBloom.prototype.add = function(key) {
    var f = this.filters.slice(-1)[0];
    if(f.add(key)) {
        return;
    }
    f = new SafeBloom(f.capacity * this.scaling, f.errorRate * this.ratio);
    f.add(key);
    this.filters.push(f);
};

ScalingBloom.prototype.has = function(key) {
    for(var i = this.filters.length; i-- > 0;){
        if(this.filters[i].has(key)){
            return true;
        }
    }
    return false;
};

ScalingBloom.prototype.serialize = function(){
    var args = [this.errorRate, this.options],
        bufs = [],
        counts = [];

    this.filters.forEach(function(filter){
        bufs.push(filter.filter.bitfield.buffer.toString('base64'));
        counts.push(filter.count);
    });
    args.push(bufs);
    args.push(counts);

    return new Buffer(JSON.stringify(args)).toString('binary');
};

ScalingBloom.unserialize = function(buf){
    var data = JSON.parse(buf.toString()),
        buffers = data[2].map(function(buffer){
            return new Buffer(buffer, 'base64');
        }),
        b;
    b = new ScalingBloom(data[0], data[1], buffers, data[3]);
    return b;
};

module.exports = ScalingBloom;
module.exports.Bloom = Bloom;
module.exports.SafeBloom = SafeBloom;
module.exports.ScalingBloom = ScalingBloom;
module.exports.calculateSize   = calculateSize;
module.exports.calculateSlices = calculateSlices;