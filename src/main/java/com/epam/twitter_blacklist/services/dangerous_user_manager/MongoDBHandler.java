package com.epam.twitter_blacklist.services.dangerous_user_manager;

import com.epam.twitter_blacklist.models.SuspiciousActivity;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;


@SuppressWarnings("ALL")
public class MongoDBHandler {

    public static void writeToMongo(SuspiciousActivity suspiciousActivity, MongoDatabase twitterBlacklistDB) {
//        MongoDatabase twitterBlacklistDB = mongoClient.getDatabase("Twitter");
        MongoCollection<Document> userTweetCollection = twitterBlacklistDB.getCollection("BlacklistUserByGroupTwits");

        //Reading documents from MongoDB - if already exist: update, else: insert
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("user_id", suspiciousActivity.getUserId());
        searchQuery.put("group", suspiciousActivity.getCategory());

        Mongo mongoClient = new Mongo("localhost", 27017);
        DB database = mongoClient.getDB("Twitter");
        DBCollection collection = database.getCollection("BlacklistUserByGroupTwits");
        DBCursor cursor = collection.find(searchQuery);

        if (cursor.hasNext()) {
            // update
            //System.out.println(cursor.next());
            BasicDBObject newDocument = new BasicDBObject();

            DBObject nextObject = cursor.next();

            Integer count = (Integer) nextObject.get("count");
            Integer newCount = count + 1;
            newDocument.put("count", newCount);

            Object message = nextObject.get("messages");
            List<String> messages = new ArrayList<>();
            if (message instanceof List<?>) {//(message.getClass() == List<String>.class) { // instanceof
                System.out.println("This is an Array");
                messages = ((List<String>) message);
            messages = (List<String>) message;
            } else { // x instanceof String
                System.out.println("This is only a string");
                messages.add((String) message);
            }

            messages.add(suspiciousActivity.getMessage());


            newDocument.put("messages", messages);

            BasicDBObject updateObject = new BasicDBObject();
            updateObject.put("$set", newDocument);

            collection.update(searchQuery, updateObject);

        } else {
            // add
            insertOneDocument(userTweetCollection, suspiciousActivity);
        }


        //insertOneDocument(userTweetCollection, suspiciousActivity);
            //insertManyDocuments(userTweetCollection, suspiciousActivity);
    }
    private static void insertOneDocument(MongoCollection<Document> userTweetCollection, SuspiciousActivity suspiciousActivity) {
        userTweetCollection.insertOne(generateNewSuspiciousAsDangerousUser(suspiciousActivity));
        System.out.println("One grade inserted for studentId " + suspiciousActivity.getUserId());
    }

//    private static void insertManyDocuments(MongoCollection<Document> userTweetCollection, Dataset<Row> parsedJsonDF) {
//        List<Document> grades = new ArrayList<>();
//        for (int i = 1; i <= 10; i++) {
//            grades.add(generateNewSuspiciousAsDangerousUser(10001d, "God" + i, "Morder", "mes " + i));
//        }
//        userTweetCollection.insertMany(grades, new InsertManyOptions().ordered(false));
//        System.out.println("Ten grades inserted for studentId 10001.");
//    }

    private static Document generateNewSuspiciousAsDangerousUser(SuspiciousActivity suspiciousActivity) {
        List<Document> messages = Collections.singletonList(new Document("message", suspiciousActivity.getMessage()));

        return new Document("_id", new ObjectId())
                .append("user_id", suspiciousActivity.getUserId())
                .append("name", suspiciousActivity.getUserFullName())
                .append("group", suspiciousActivity.getCategory())
                .append("count", 1)
                .append("messages", suspiciousActivity.getMessage());
    }
}