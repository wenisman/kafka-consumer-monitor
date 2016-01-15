var nodeCache   = require('node-cache');
var Promise     = require('promise');

var cache = new nodeCache();

function getContents() {
    return new Promise(function(resolve, reject){
        cache.keys(function(err, keys){
            cache.mget(keys, function(err, items){
                return resolve({keys: keys, items: items});
            });
        });
    });
}

module.exports = {
    cache : cache,
    getContents: getContents,
    get : cache.get,
    set : cache.set,
    keys : cache.keys
};
