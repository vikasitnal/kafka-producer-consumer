(function(notification) {
var kafka = require('kafka-node');
//var emailService = require('emailService');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var Q = require('q');
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
    var skuId = 0;
    var notificationType = message.topic;
    if(notificationType == 'order')
    {
        if(message.key == 'orderId'){
            orderId = message.value;
           // console.log('orderID '+message.value);
        }
        if(message.key == 'productId'){
            productId = message.value;
       //  console.log('productId '+message.value);    
         }
         if(message.key == 'userId'){
             userId = message.value;
         }
         if(message.key == 'skuId'){
            skuId = message.value;
        }
        notification.processNotification(userId,productId,orderId,skuId,notificationType);
    }
});

notification.processNotification = function(userId,productId,orderId,skuId,notificationType){
    var User = {};
    var Order = {};
    var Invoice = {};
    var template = {};
    var filledTemplate = {};
    notification.getUser(userId)
    .then(function(user){
        console.log('Got the User');
        User = user;
        return notification.getOrder(orderId)
    })
    .then(function(order){
        console.log('Got the Order')
        Order = order;
        return notification.getInvoice(orderId,skuId)
    })
    .then(function(invoice){
        console.log('Got the Invoice');
        Invoice = invoice;
        return notification.getTemplate(notificationType,invoice)
    })
    .then(function(template){
        console.log('Got the template');
        return notification.populateTemplate(template,User,Order,Invoice)
    })
    .then(function(filledTemplate){
        console.log('Template Populated');
        return notification.sendNotification(filledTemplate,User);
    })
}

notification.sendNotification = function(filledTemplate,User){
    console.log('notification sent');
}

notification.populateTemplate = function(template,User,Order,Invoice){
    
}

notification.getTemplate = function(notificationType,invoice){
    var deferred = Q.defer();
    if(invoice){
        deferred.resolve('abc');
    }
    else{
        deferred.resolve('def');
    }
    return deferred.promise;
}

notification.getInvoice = function(){
    var deferred = Q.defer();
    var invoice = {
        article:"12323434",
        dateTime:"12/12/2012 12:24:02PM"
    }
    deferred.resolve(invoice);
    return deferred.promise;
}

notification.getOrder = function(orderId){
    var deferred = Q.defer();
    var order = {
        dateTime:"12/12/2012 12:23:34PM",
    }
    deferred.resolve(order);
    return deferred.promise;
}

notification.getUser = function(userId){
    var deferred = Q.defer();
    var user = {
        userEmail : 'abc@def.com',
        deviceId  : 'kjsdhkjhfd76sdlkjf',
        mobileNumber: '9873932798'
    };
    deferred.resolve(user);
    return deferred.promise;
}



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
});})(module.exports);