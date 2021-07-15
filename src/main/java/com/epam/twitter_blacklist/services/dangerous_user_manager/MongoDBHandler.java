package com.epam.twitter_blacklist.services.dangerous_user_manager;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.bson.types.ObjectId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static java.util.Arrays.asList;


public class MongoDBHandler {

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false")) {
            MongoDatabase twitterBlacklistDB = mongoClient.getDatabase("Twitter");
            MongoCollection<Document> userTwitCollection = twitterBlacklistDB.getCollection("BlacklistUserByGroupTwits");
            insertOneDocument(userTwitCollection);
            insertManyDocuments(userTwitCollection);
        }
    }
    private static void insertOneDocument(MongoCollection<Document> userTwitCollection) {
        userTwitCollection.insertOne(generateNewSuspiciousUser(10000d, "Gad", "Morder", "mes"));
        System.out.println("One grade inserted for studentId 10000.");
    }
    private static void insertManyDocuments(MongoCollection<Document> userTwitCollection) {
        List<Document> grades = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            grades.add(generateNewSuspiciousUser(10001d, "God" + i, "Morder", "mes " + i));
        }
        userTwitCollection.insertMany(grades, new InsertManyOptions().ordered(false));
        System.out.println("Ten grades inserted for studentId 10001.");
    }
    private static Document generateNewSuspiciousUser(double userId, String userName, String group, String message) {
        List<Document> messages = Collections.singletonList(new Document("message", message));

        return new Document("_id", new ObjectId())
                .append("user_id", userId)
                .append("name", userName)
                .append("group", group)
                .append("count", 1)
                .append("messages", messages);
    }
}