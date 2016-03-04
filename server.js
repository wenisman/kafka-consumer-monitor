var zkLib       = require('./lib/ZooKeeper.js');
var kafkaLib    = require('./lib/Kafka.js');
var cache       = require('./lib/Cache.js');
var logger      = require('./logger.js').logger;
var config      = require('./config');
var eventEmitter = require('events');
var express     = require('express');
var Promise     = require('promise');
var app         = express();

var loadMonitorData = function() {
    logger.trace('loading consumer metadata');
    return zkLib
        .loadConsumerMetaData()
        .then(kafkaLib.getTopicOffsets)
        .then(function() {
            cache
                .getContents()
                .then(function(result){
                    logger.info(result.items, "consumer lags");
                });
        })
        .then(function() {
            setTimeout(function() {
                loadMonitorData();
            }, config.refreshInterval);
        });
};

// set a delay to stop zk throwing errors from us trying to connect to quickly
loadMonitorData();

// the zookeeper session has expired, we need to stop the application and let the systemd
// restart the application for a clean restart
var emitter     = new eventEmitter();
emitter.on('session_expired', function() {
    process.exit(15);
});


// CORS headers for external access from JS
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});


app.get('/monitor/refresh', function(req, res) {
    logger.trace('load metadata called from external source');
    loadMonitorData()
        .then(function(){
            logger.trace('completed loading metadata');
            return res.status(200).send();
        })
        .catch(function(err){
            if (err) {
                return res.status(500).json({'error_code': 500, 'message': err});
            }
        });
});

app.get('/consumergroups', function(req, res){
    cache.keys(function(err, keys){
        logger.debug({keys: keys}, 'returned keys');
        res.status(200).send(keys);
    });
});


app.get('/consumers/:consumer/lag', function(req, res) {
    var consumer = req.params.consumer;

    cache.get(consumer, function(err, value){
        if (!err) {
            // TODO : sort the response here if required
            res.send(value);
        }
        else {
            res.status(500).send();
        }
    });
});

app.listen(config.server.port);
