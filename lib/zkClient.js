var zk          = require('node-zookeeper-client');
var config      = require('../config.js');

var logger      = require('../logger.js').logger;
var Promise     = require('promise');

var zkClientState = zk.State;
var zkClient    = zk.createClient(process.env.ZOOKEEPER_CONNECT || config.zkConnect);

var eventEmitter = require('events');
var emitter     = new eventEmitter();

var disconnectZookeeper = function() {
    return new Promise(function(resolve, reject) {
        if (zkClient.getState !== zkClientState.SYNC_DISCONNECTED) {
            logger.trace('closing open connections to zookeeper');
            zkClient.close();
        }
        else {
            logger.trace('no open connections to zookeeper');
        }

        logger.trace('closed connections to zookeeper');
        resolve();
    });
};

var connectZooKeeper = function() {
    logger.trace('connecting to zookeeper');

    return new Promise(function(resolve, reject) {
        if (zkClient.getState() === zkClientState.SYNC_CONNECTED) {
            logger.trace('Connection to zk already open');
            return resolve();
        }

        zkClient.once('expired', function() {
            logger.trace('zookeeper session expired, emit the state change');
            emitter.emit('session_expired');
            return;
        });

        zkClient.once('connected', function () {
            logger.trace('connection to zookeeper established');
            return resolve();
        });

        logger.trace({state: zkClient.getState()}, 'no other connections, creating connection to zookeeper');
        zkClient.connect();
    });
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

var getState = function() {
    return zkClient.getState();
};

module.exports = {
    getChildren : denodeify(zkClient.getChildren, zkClient),
    getData : denodeify(zkClient.getData, zkClient),
    remove : denodeify(zkClient.remove, zkClient),
    close : denodeify(zkClient.close, zkClient),
    getState : getState,
    connectZooKeeper : connectZooKeeper,
    disconnectZookeeper : disconnectZookeeper
};
