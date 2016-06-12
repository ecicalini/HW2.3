package com.mongodb.m101j.homework.three.one;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UnwindOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.m101j.util.Helpers.printJson;

/**
 * Write a program in the language of your choice that will remove the lowest homework score for each student. Since
 * there is a single document for each student containing an array of scores, you will need to update the scores array
 * and remove the homework.
 *
 * <b>Remember, just remove a homework score. Don't remove a quiz or an exam!</b>
 *
 * Hint/spoiler: With the new schema, this problem is a lot harder and that is sort of the point. One way is to find
 * the lowest <b>homework</b> in code and then update the scores array with the low homework pruned.
 *
 * To confirm you are on the right track, here are some queries to run after you process the data with the correct
 * answer shown:
 *
 * Let us count the number of students we have:
 *
 * > use school
 * > db.students.count()
 *
 * The answer will be 200.
 *
 * Let's see what Tamika Schildgen's record looks like once you have removed the lowest score:
 *
 * >db.students.find( { _id : 137 } ).pretty( )
 *
 * This should be the output:
 * {
 * "_id" : 137,
 * "name" : "Tamika Schildgen",
 * "scores" : [
 * {
 *"type" : "exam",
 *"score" : 4.433956226109692
 *},
 *{
 *"type" : "quiz",
 *"score" : 65.50313785402548
 *},
 *{
 *"type" : "homework",
 *"score" : 89.5950384993947
 *}
 *]
 *}
 *
 * To verify that you have completed this task correctly, provide the identity (in the form of their _id) of the student
 * with the highest average in the class with following query that uses the aggregation framework.
 * The answer will appear in the _id field of the resulting document.
 *
 *> db.students.aggregate( [
                             { '$unwind': '$scores' },
                             {
                             '$group':
                             {
                             '_id': '$_id',
                             'average': { $avg: '$scores.score' }
                             }
                             },
                             { '$sort': { 'average' : -1 } },
                             { '$limit': 1 } ] )
 *
 *1
 *
 * Created by elena on 10/06/16.
 */
public class RemoveLowestHomeworkScore {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("school");
        MongoCollection<Document> collection = database.getCollection("students");

//        db.students.aggregate(
//                // Expand the scores array into a stream of documents
//                { $unwind: '$scores' },
//
//        // Filter to 'homework' scores
//        { $match: {
//            'scores.type': 'homework'
//
//        }},
//
//        // Sort in descending order
//        { $sort: {
//            '_id' : 1,
//            'scores.score': 1
//        }}
//        ).pretty()

        //find all homework to remove
        List<BasicDBObject> aggregation = Arrays.asList(
                new BasicDBObject("$unwind", "$scores"),
                new BasicDBObject("$match", new BasicDBObject("scores.type", "homework")),
                new BasicDBObject("$sort", new BasicDBObject("_id", 1).append("scores.score", 1))
        );

        List<Document> hwToDelete = new ArrayList<Document>();
        collection.aggregate(aggregation).into(hwToDelete);

        Integer student_id = null;
        for (Document hwScore : hwToDelete) {
            Integer current_student_id = (Integer) hwScore.get("_id");
            Document scores = (Document) hwScore.get("scores");
            Double score = (Double) scores.get("score");
            String type = (String) scores.get("type");

            System.out.println("hwScore.get(\"scores.type\") = " + scores);
            System.out.println("scores.get(\"score\") = " + score);
            System.out.println("scores.get(\"type\") = " + type);

            if(student_id == null || !student_id.equals(current_student_id)){
                student_id = current_student_id;
                BasicDBObject query = new BasicDBObject("_id", current_student_id);
                BasicDBObject fields = new BasicDBObject("scores", new BasicDBObject("type", type).append("score", score));
                BasicDBObject update = new BasicDBObject("$pull",fields);

                collection.updateOne( query, update );
            }
        }

        List<Document> id = collection.find(eq("_id", 137)).into(new ArrayList<Document>());
        printJson(id.get(0));


    }
}
