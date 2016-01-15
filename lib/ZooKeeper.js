'use strict';

var zk          = require('node-zookeeper-client');
var _           = require('lodash');
var config      = require('../config.js');
var cache       = require('./Cache.js');
var logger      = require('../logger.js').logger;
var Promise     = require('promise');

var zkClientState = zk.State;
var zkClient    = null;

zkClient = zk.createClient(process.env.ZOOKEEPER_CONNECT || config.zkConnect);
var connectZooKeeper = function() {
    logger.trace('connecting to zookeeper');
    if (zkClient.getState() === zkClientState.SYNC_CONNECTED) {
        logger.trace('Connection to zk already open');
        return;
    }

    zkClient.once('connected', function () {
        logger.trace('connection to zookeeper established');
        return;
    });

    zkClient.connect();
};

connectZooKeeper();

var denodeify = function (f, that) {
    return function () {
        var args = Array.prototype.slice.call(arguments);

        return new Promise(function (resolve, reject) {
            f.apply(that, args.concat(function (err, data, status) {
                if (err) {
                    logger.error({err:err, args: args }, 'denodify error');
                    return reject(err);
                }
                resolve({ data: data, status: status, args: args });
            }));
        });
    };
};

var getChildren = denodeify(zkClient.getChildren, zkClient);
var getData = denodeify(zkClient.getData, zkClient);
var remove = denodeify(zkClient.remove, zkClient);
var close = denodeify(zkClient.close, zkClient);

var getOwner = function(consumer, topic, partition) {
    var ownerUri = '/consumers/' + consumer + '/owners/' + topic + '/' + partition;
    logger.trace({uri: ownerUri}, 'begin getting owner from zookeeper');

    return getData(ownerUri)
            .then(function(result){
                logger.trace({owner: result.data.toString(), uri: ownerUri}, 'registered owner');
                return result.data.toString();
            })
            .catch(function(err){
                logger.warn({err: err, uri: ownerUri}, 'error retrieving the owner data');
            });
};


var getOffset = function(consumer, topic, partition) {
    var offsetUri = '/consumers/' + consumer + '/offsets/' + topic + '/' + partition;
    logger.trace({uri: offsetUri}, 'begin getting offset from zookeeper');
    return getData(offsetUri)
            .then(function(result){
                logger.trace({offset: result.data.toString(), uri: offsetUri}, 'registered offset');
                return result.data.toString();
            })
            .catch(function(err){
                logger.warn({err: err, uri: offsetUri}, 'error retrieving the offset data');
            });
};

var loadPartitions = function(consumer, topic, partitions) {
    logger.trace({consumer : consumer, topic: topic, partitions: partitions}, 'begin loading partitions');
    return Promise.all(
        partitions.map(function(partition){
            var item = {consumer: consumer, topic: topic, partition: partition};
            return getOffset(consumer, topic, partition)
                .then(function(offset){
                    item.offset = offset;
                    logger.trace({item: item}, 'registered offset');
                })
                .then(function(){
                    return getOwner(consumer, topic, partition);
                })
                .then(function(owner){
                    item.owner = owner;
                    logger.trace({item: item}, 'registered owner');
                })
                .catch(function(err){
                    logger.warn(err, 'error loading partitions');
                })
                .then(function(){
                    return item;
                });
        })
    );
};

var getPartitions = function(consumer, topic) {
    var partitionUri = '/consumers/' + consumer + '/offsets/' + topic;
    logger.trace({uri: partitionUri}, 'begin getting partitions from zookeeper');
    return getChildren(partitionUri)
            .then(function(result){
                logger.trace({partitions: result.data, uri: partitionUri}, 'registered partitions');
                return loadPartitions(consumer, topic, result.data);
            })
            .then(function(result) {
                logger.trace({result: result, uri: partitionUri}, 'completed getting partitions');
                return result;
            });

};

var loadTopics = function(consumer, topics) {
    logger.trace({consumer : consumer, topics: topics}, 'begin loading topics');
    return Promise.all(
        topics.map(function(topic){
            return getPartitions(consumer, topic);
        })
    );
};

var getTopics = function(consumer) {
    var topicUri = '/consumers/' + consumer + '/offsets';
    logger.trace({uri: topicUri}, 'begin getting topics from zookeeper');
    return getChildren(topicUri)
            .then(function(result) {
                logger.trace({ topics: result.data, uri: topicUri}, 'registered topics');
                return loadTopics(consumer, result.data);
            })
            .then(function(result){
                logger.trace({result: result, uri: topicUri}, 'completed getting topics');
                return result;
            }); 

};

var filterConsumers = function(consumers) {
    return consumers.filter(function(consumer){
        return (consumer.indexOf('schema-registry') == -1); 
    });
};

var loadConsumers = function(consumers) {
    logger.trace({consumers : consumers}, 'begin loading consumers');
    return Promise.all(
        consumers.map(function(consumer){
            return getTopics(consumer)
                .catch(function(err){
                    logger.warn({err:err, consumer: consumer}, 'error loading consumer');
                })
                .then(function(result){ 
                    if (!result) {
                        return [{consumer: consumer, topic: '', partition: ''}]; 
                    }

                    return result;
                });
        })
    ).then(function(result) {
        logger.trace({result: result, consumers: consumers}, 'completed loading consumers');
        return result;
    });
};

var setConsumerCache = function(consumers) {
    logger.trace({consumers: consumers}, 'saving the consumers to the cache');
    return Promise.all(
        consumers.map(function(consumer){
            if (!!consumer) {
                consumer = _.flatten(consumer);
                logger.trace(consumer, 'flattened consumer');
                cache.set(consumer[0].consumer, consumer);
            }
        })
    );
};

// use this if you want to watch the consumers in zookeeper. can make for a chatty app
var getConsumers = function() {
    var consumersUri = '/consumers';
    logger.trace({uri: consumersUri}, 'begin loading consumers');

    return getChildren(consumersUri)
            .then(function(result) {
                logger.trace(result.data, 'registered consumers');
                return filterConsumers(result.data);
            })
            .then(loadConsumers)
            .then(setConsumerCache)
            .catch(function(err){
                logger.warn({consumersUri: consumersUri, err: err}, 'error returning the consumers from Zookeeper');
            }) 
            .then(function(result) {
                return result;
            });
};


module.exports = {
    loadConsumerMetaData : function() {
        connectZooKeeper(); 

        return getConsumers();
    }
};
