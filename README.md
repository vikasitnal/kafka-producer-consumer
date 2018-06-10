# kafka-producer-consumer
notification processor 

producer.js
- This is a producer that publishes messages to a topic named 'Order'
- This publishes an array as payload. 
- Array is consists of keyedMessages, One containing the orderId,produtId and userId.

consumer.js
- This is to demonstrate a sample conusmer which has subscribed to the topic 'Order'
- It just updates the order object

consumer-notification.js
- This is also an example of consumer to the topic 'Order'.
- This gets User,Order,Invoice and sends emails,message,push notification.
