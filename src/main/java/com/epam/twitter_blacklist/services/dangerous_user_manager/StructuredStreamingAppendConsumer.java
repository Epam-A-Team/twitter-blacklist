package com.epam.twitter_blacklist.services.dangerous_user_manager;

import com.epam.twitter_blacklist.models.SuspiciousActivity;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

        String bootstrapServer = "34.134.45.229:9092";

        SparkSession spark = SparkSession.builder()
                .appName("append application")
                .master("local[*]")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/TwitterBlacklist.BlacklistTwits")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/TwitterBlacklist.BlacklistTwits")
                .getOrCreate();

        Dataset<Row> inputSuspiciousDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("subscribe", "user-suspicious-activity-topic")
                .option("startingOffsets", "earliest")
                .load();

        inputSuspiciousDF = inputSuspiciousDF.select(col("value").cast(StringType).alias("value"));

        StructType jsonSchema = Encoders.bean(SuspiciousActivity.class).schema();


        Dataset<Row> parsedJsonDF = inputSuspiciousDF
                .withColumn("parsed_json", from_json(col("value"), jsonSchema))
                .select("parsed_json.*");

        // filter specific data from input (from Kafka)
        Dataset<Row> outputDF = parsedJsonDF.select("userFullName","message");

        // Write result of reading from topic user-suspicious-activity-topic to topic structured-streaming-result in Kafka
        StreamingQuery query = outputDF.toJSON()
                .writeStream()
                .format("kafka")
                .option("checkpointLocation", "C:\\Users\\anafa\\Documents\\Twitterblacklistproject")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("topic", "structured-streaming-result")
                .outputMode(OutputMode.Append())
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        } finally {
            spark.close();
        }

    }
}