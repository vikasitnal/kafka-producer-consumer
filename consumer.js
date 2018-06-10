var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var topic = 'order';

var client = new Client('localhost:2181');
var topics = [{ topic: topic, partition: 0 }];
var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
   var orderId = 0;
   var productId = 0;
   var userId = 0;
   if(message.key == 'orderId'){
       orderId = message.value;
       console.log('orderID '+message.value);
   }
   if(message.key == 'productId'){
       productId = message.value;
    console.log('productId '+message.value);    
    }
    if(message.key == 'userId'){
        userId = message.value;
    }
    console.log('Order Updated');
});

consumer.on('error', function (err) {
  console.log('error', err);
});

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
  topic.maxNum = 2;
  offset.fetch([topic], function (err, offsets) {
    if (err) {
      return console.error(err);
    }
    var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
    consumer.setOffset(topic.topic, topic.partition, min);
  });
});