package org.example;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class SparkToMongoDB {
    public static void main(String[] args) {
        try{

//======================================[ MongoDB connection ]=============================================================

            // mongodb atlus connection...
            SparkSession spark = SparkSession.builder()
                    .master("local")
                    .appName("MongoDBConnection")
                    .config("spark.mongodb.input.uri", "mongodb+srv://vishalpasi:FbiA1ChEDTbvv6eL@cluster0.3xmrakz.mongodb.net/SparkData.User")
                    .config("spark.mongodb.output.uri", "mongodb+srv://vishalpasi:FbiA1ChEDTbvv6eL@cluster0.3xmrakz.mongodb.net/SparkData.User")
                    .getOrCreate();

            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

            // Read....
            Dataset<Row> df = MongoSpark.load(jsc).toDF();
            df.show();

            // write...
            List<Document> documents = new ArrayList<>();
            documents.add(new Document("fname", "John").append("lname","Doe").append("age", 30));
            documents.add(new Document("fname", "Jane").append("lname","Doe").append("age", 25));
            documents.add(new Document("fname", "June").append("lname","Doe").append("age", 25));


            WriteConfig writeConfig = WriteConfig.create(jsc);

            MongoSpark.save(jsc.parallelize(documents), writeConfig);

            jsc.close();


            //.............Join................

//            List<Document> documents = new ArrayList<>();
//            documents.add(new Document("fname", "John").append("lname","Doe").append("age", 30));
//            documents.add(new Document("fname", "Jane").append("lname","Doe").append("age", 25));
//            documents.add(new Document("fname", "June").append("lname","Doe").append("age", 25));
//
//            List<UserModel> userModelList = new ArrayList<>();
//            userModelList.add(new UserModel("123","June","pasi","vishal@gmail","1234"));
//            userModelList.add(new UserModel("123","vishal","pasi","vishal@gmail","1234"));
//            userModelList.add(new UserModel("123","vishal","pasi","vishal@gmail","1234"));
//
//            Dataset<Row> df = MongoSpark.load(jsc).toDF();
//            Dataset<UserModel> peopleDataset = spark.createDataset(userModelList, Encoders.bean(UserModel.class));

//          left join....

//            Dataset<Row> resultL=df.join(peopleDataset, df.col("fname").equalTo(peopleDataset.col("fname")), "left");
//            resultL.show();

//          right join....
//            Dataset<Row> resultL=df.join(peopleDataset, df.col("fname").equalTo(peopleDataset.col("fname")), "right");
//            resultL.show();

//        outer join....
//            Dataset<Row> result2=df.join(peopleDataset, df.col("fname").equalTo(peopleDataset.col("fname")), "outer");
//            result2.show();

            // Inner Join.....
//            Dataset<Row> result=df.join(peopleDataset, df.col("fname").equalTo(peopleDataset.col("fname")), "inner");
//            result.show();


            // full join....
//            Dataset<Row> result1=df.join(peopleDataset, df.col("fname").equalTo(peopleDataset.col("fname")), "full");
//            result1.show();


        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
