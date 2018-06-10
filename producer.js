var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    client = new kafka.Client(),
    producer = new Producer(client);
    var km = new KeyedMessage('orderId', '123123');
    var km1 = new KeyedMessage('productId',1321321);
    var km2 = new KeyedMessage('userId',132);
    var payloads = [
        { topic: 'order', messages: [km,km1,km2], partition: 0 }
    ];
producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
});
 
producer.on('error', function (err) {})
