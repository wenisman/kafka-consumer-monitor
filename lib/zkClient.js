var zk          = require('node-zookeeper-client');
var config      = require('../config.js');

var logger      = require('../logger.js').logger;
var Promise     = require('promise');

var zkClientState = zk.State;
var zkClient    = zk.createClient(process.env.ZOOKEEPER_CONNECT || config.zkConnect);

var connectZooKeeper = function(callback) {
    logger.trace('connecting to zookeeper');
    if (zkClient.getState() === zkClientState.SYNC_CONNECTED) {
        logger.trace('Connection to zk already open');

        if (!!callback) { callback(); }
        return;
    }

    zkClient.once('connected', function () {
        logger.trace('connection to zookeeper established');
        if (!!callback) { callback(); }
        return;
    });

    zkClient.connect();
    if (!!callback) { callback(); }
};

var denodeify = function (f, that) {
    return function () {
        var args = Array.prototype.slice.call(arguments);

        return new Promise(function (resolve, reject) {
            f.apply(that, args.concat(function (err, data, status) {
                if (err) {
                    logger.debug({err:err, args: args }, 'denodify error');
                    return reject(err);
                }
                resolve({ data: data, status: status, args: args });
            }));
        });
    };
};

module.exports = {
    getChildren : denodeify(zkClient.getChildren, zkClient),
    getData : denodeify(zkClient.getData, zkClient),
    remove : denodeify(zkClient.remove, zkClient),
    close : denodeify(zkClient.close, zkClient),
    connectZooKeeper : connectZooKeeper
};
