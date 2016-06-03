/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.m101j.crud;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.m101j.util.Helpers.printJson;

public class App {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("students");
        MongoCollection<Document> collection = database.getCollection("grades");

        // db.grades.find({type:"homework"}).sort({student_id:1},{score:1})
        Bson filter = eq("type", "homework");

        Bson sort = Sorts.ascending("student_id", "score");

        List<Document> students = collection.find(filter).sort(sort).into(new ArrayList<Document>());

        System.out.println("grades collection initial size: " + collection.count());

        Integer student_id = null;
        for (Document student : students) {
            Integer current_student_id = (Integer) student.get("student_id");
            if(student_id == null){
                student_id = current_student_id;
                collection.deleteOne(student);
            }

            if(!student_id.equals(current_student_id)){
                student_id = current_student_id;
                collection.deleteOne(student);
            }
        }

        System.out.println("grades collection final size: " + collection.count());

//        db.grades.find().sort({'score':-1}).skip(100).limit(1)
        List<Document> score = collection.find().sort(descending("score"))
                                                .skip(100)
                                                .limit(1)
                                                .into(new ArrayList<Document>());
        System.out.println("== db.grades.find().sort({'score':-1}).skip(100).limit(1) ==");
        printJson(score.get(0));

//        db.grades.find({},{'student_id':1, 'type':1, 'score':1, '_id':0}).sort({'student_id':1, 'score':1, }).limit(5)
        List<Document> score5 = collection.find().projection(fields(include("student_id", "type", "score"), excludeId()))
                .sort(ascending("student_id","score"))
                .limit(5)
                .into(new ArrayList<Document>());

        System.out.println("== db.grades.find({},{'student_id':1, 'type':1, 'score':1, '_id':0}).sort({'student_id':1, 'score':1, }).limit(5) ==");
        for (Document doc : score5) {
            printJson(doc);
        }

    }
}
