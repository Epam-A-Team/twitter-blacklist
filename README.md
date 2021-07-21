Connection to MongoDB:
mongodb://127.0.0.1:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false

Current ExternalIP:
34.69.211.48

Suspicious activity topic name:
user-suspicious-activity-topic

Suspicious as dangerous user topic name:
structured-streaming-result

Getting data from specific topic in Kafka:
(user-suspicious-activity-topic) kafka-console-consumer --bootstrap-server cnt7-naya-cdh63:9092 --topic user-suspicious-activity-topic
(structured-streaming-result) kafka-console-consumer --bootstrap-server cnt7-naya-cdh63:9092 --topic structured-streaming-result
(structured-mongo-and-kafka-streaming-user-tweets) kafka-console-consumer --bootstrap-server cnt7-naya-cdh63:9092 --topic structured-mongo-and-kafka-streaming-user-tweets
