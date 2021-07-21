package com.epam.twitter_blacklist.services.dangerous_user_manager;

import com.epam.twitter_blacklist.models.SuspiciousActivity;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.SneakyThrows;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class StructuredStreamingAppendConsumer {

    @SneakyThrows
    public static void main(String[] args){
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master");

        String bootstrapServer = "34.69.211.48:9092";

        SparkSession spark = SparkSession.builder()
                .appName("append application")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> inputSuspiciousDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("subscribe", "user-suspicious-activity-topic")
                .option("startingOffsets", "earliest")
                .load()
                .select(col("value").cast(StringType).alias("value"));


        StructType jsonSchema = Encoders.bean(SuspiciousActivity.class).schema();


        Dataset<Row> parsedJsonDF = inputSuspiciousDF
                .withColumn("parsed_json", from_json(col("value"), jsonSchema))
                .select("parsed_json.*");

        Dataset<SuspiciousActivity> out = parsedJsonDF.as(Encoders.bean(SuspiciousActivity.class));

        StreamingQuery query = out
                .writeStream()
                .outputMode(OutputMode.Append())
                .foreachBatch((ds, batchId) -> {
                    ds.foreachPartition(part -> {
                        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false");
                        MongoDatabase twitterBlacklistDB = mongoClient.getDatabase("Twitter");

                        while (part.hasNext()){
                            MongoDBHandler.writeToMongo(part.next(), twitterBlacklistDB);
                        }

                        mongoClient.close();
                    });
                }).start();

        // Write result of reading from topic user-suspicious-activity-topic to topic structured-streaming-result in Kafka
//        StreamingQuery query = parsedJsonDF.toJSON()
//                .writeStream()
//                .format("kafka")
//                .option("checkpointLocation", "C:\\Users\\anafa\\Documents\\Twitterblacklistproject")
//                .option("kafka.bootstrap.servers", bootstrapServer)
//                .option("topic", "structured-streaming-result")
//                .outputMode(OutputMode.Append())
//                .start();

        // Writing to MongoDB
        //MongoDBHandler.writeToMongo(parsedJsonDF);

        /*
        // ******************************************************************
        System.out.println("*******************************************************");
        System.out.println("*********** Mongo with Kafka **************************");
        System.out.println("*******************************************************");
        // Reading from MongoDB
        Dataset<Row> loadMongo = spark
                .read()
                .format("mongo")
                .option("uri", "mongodb://localhost:27017/Twitter.BlacklistUserByGroupTwits")
                .load();
        System.out.println("Mongo!");
        loadMongo.printSchema();
        //loadMongo.show();

        loadMongo.createOrReplaceTempView("USERTWETTSBYGROUP");
        outputDF.createOrReplaceTempView("CHANGES");


//        Dataset<Row> sql = spark.sql("select * from USERTWETTSBYGROUP utg,CHANGES ch where  ch.userFullName == utg.name and ch.category == utg.group");
        Dataset<Row> sql = spark.sql("select * from USERTWETTSBYGROUP utg,CHANGES ch where utg.user_id == 10000");
        System.out.println("Mongo and Kafka");
        sql.printSchema();

        // writing results to new topic
        Dataset<Row> newOutputAsJsonDF = sql.toJSON().toDF();
        StreamingQuery mongoKafkaQuery = newOutputAsJsonDF.toJSON()
                .writeStream()
                .format("kafka")
                .option("checkpointLocation", "C:\\Users\\anafa\\Documents\\Twitterblacklistproject_Mongo_with_Kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("topic", "structured-mongo-and-kafka-streaming-user-tweets")
                .outputMode(OutputMode.Append())
                .start();
        System.out.println("*******************************************************");
*/

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        } finally {
            spark.close();
        }

    }
}