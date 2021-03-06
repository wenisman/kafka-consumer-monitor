var kafka       = require('kafka-node');
var config      = require('../config.js');
var cache       = require('./Cache.js');
var logger      = require('../logger.js').logger;
var Promise     = require('promise');

var kafkaClient = new kafka.Client(process.env.ZOOKEEPER_CONNECT || config.zkConnect, 'lag-monitor');
var kafkaOffset = new kafka.Offset(kafkaClient);

var getOffsetFetch = function(cb) {

    return cache.getContents().
        then(function(result){
            logger.trace(result, 'cached consumers');

            var map = [];
            result.keys.forEach(function(key) {
                var consumer = result.items[key];
                if (!!consumer) {
                    logger.trace(consumer, 'offset consumer to update is');
                    map = map.concat(consumer.map(function(consumertopic){
                        return {
                            topic: consumertopic.topic,
                            partition: consumertopic.partition,
                            time: -1,
                            maxNum: 1 };
                    }));
                }
            });
            logger.trace(map, 'mapped consumers');
            return map;
        });
};


var updateCachedItems = function(updates) {
    logger.trace({updates: updates},'updates to be cached');
    if (!updates || updates.length === 0) {
        return;
    }


    return cache.getContents()
        .then(function(results){
            return Promise.all(
                results.keys.map(function(key){
                    var consumer = results.items[key];
                    logger.trace({consumer: consumer, key: key}, 'update consumer offset');

                    if (!!consumer) {
                        consumer.forEach(function(consumerTopic){
                            logger.trace({topic: consumerTopic}, 'offset to update');
                            // map the update to the consumer
                            var offset = updates.filter(function(update){
                                return (!!update && update[consumerTopic.topic] && update[consumerTopic.topic][consumerTopic.partition]);
                            });
                            if (offset.length > 0) {
                                offset = offset[0][consumerTopic.topic][consumerTopic.partition];

                                logger.trace({topic: consumerTopic, offset: offset}, 'offset to update');
                                if (offset.length) {
                                    consumerTopic.end = parseInt(offset[0]);
                                    consumerTopic.lag = consumerTopic.end - consumerTopic.offset;
                                }
                            }
                        });
                        //save the consumer back to the cache
                        logger.trace({key:key, consumer:consumer}, 'saving updated consumer');
                        cache.set(key, consumer);
                    }

                    return null;
                })
            );
        });
};


var fetchOffset = function(consumer) {
    logger.trace(consumer, 'begin loading consumer offset from kafka');
    return new Promise(function(resolve, reject) {
        logger.trace(consumer, 'calling fetch');
        kafkaOffset.fetch([consumer], function(err, data){
            logger.trace(consumer, 'fetch complete');
            if (err) {
                logger.error({err:err, consumerRequest:consumer}, 'error loading offset from kafka');
                return reject({data: consumer, err:err});
            }

            logger.trace(data, 'returned items from kafka:');
            resolve(data);
        });
    });
};


var fetchKafkaOffsets = function(consumers) {
    return Promise.all(
        consumers.map(function(consumer){
            logger.trace(consumer, 'loading kafka offset for consumer');
            return fetchOffset(consumer)
                .catch(function(err){
                    logger.warn({err: err, consumer: consumer}, 'error fetching offszet from kafka');
                });
        })
    );
};

var connectKafka = function() {
    return new Promise(function(resolve, reject){
        kafkaClient = new kafka.Client(process.env.ZOOKEEPER_CONNECT || config.zkConnect, 'lag-monitor');

        kafkaClient.once('ready', function(){
            kafkaOffset = new kafka.Offset(kafkaClient);
            resolve();
        });
    });
};

var closeKafka = function() {
    return new Promise(function(resolve, reject){
        kafkaClient.close();
        resolve();
    });
};


var getTopicOffsets = function() {
    logger.trace('get the topic offsets');

    return
        connectKafka()
        .getOffsetFetch()
        .then(fetchKafkaOffsets)
        .then(updateCachedItems)
        .then(closeKafka)
        .catch(function(err){
            logger.error(err, 'error retreiving data from kafka');
        });
};


module.exports = {

    getTopicOffsets : function() {
        return getTopicOffsets();
    }
};
