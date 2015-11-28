'use strict';

const Hapi  = require('hapi');

const server = new Hapi.Server();

server.connection({port: 8000, host: 'localhost'});
server.start((err) => {

    if(err) {
        throw err;
    }

    console.log("Server running on port", server.info.uri)
    const Kafka = require('kafka-node');
    const KeyedMessage = Kafka.KeyedMessage
    const Producer = Kafka.Producer
    const Client   = Kafka.Client;
    let client     = new Client('localhost:2181');
    let producer   = new Producer(client, { requireAcks: 1})

    producer.on('ready', () => {

        let message      = 'some message without key';
        let keyedMessage = new KeyedMessage('key1', 'Keyed message');
        let topic        = 'topic1';

        setInterval(() => {
            producer.send([
                {topic: topic, partition: 0, messages: [message, keyedMessage], attributes: 0}
            ], (err, result) =>{
                console.log("In here");
                console.log(result);

            })
        }, 3000)
    })

    producer.on('error', (err) => {
        console.log('error', err);
    })

})