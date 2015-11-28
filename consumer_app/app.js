'use strict';

const Hapi  = require('hapi');

const server = new Hapi.Server();

server.connection({port: 8001, host: 'localhost'});
server.start((err) => {

    if(err) {
        throw err;
    }

    console.log("Server running on port", server.info.uri)
    const Kafka = require('kafka-node');
    const KeyedMessage = Kafka.KeyedMessage
    const Consumer = Kafka.Consumer
    const Client   = Kafka.Client;
    const Offset   = Kafka.Offset;

    let client     = new Client('localhost:2181');
    let topics     = [{topic: 'topic1', partition: 0}]
    let options    = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes:1024 * 1024 }
    let consumer   = new Consumer(client, topics, options)
    let offset     = new Offset(client)

    consumer.on('message', function(message) {
        console.log(message);
        console.log(message.key.toString('utf8'))
    })

    consumer.on('error', function(err){
        console.log('error', err)
    })

    consumer.on('offsetOutOfRange', function(topic){
        console.log('offsetOutOfRange');
        topic.maxNum = 2;
        offset.fetch([topic], (err, offsets) => {
            var min = Math.min.apply(null, offsets[topic.topic][topic.partition])
            consumer.setOffset(topic.topic, topic.partition, min);
        })
    })

})