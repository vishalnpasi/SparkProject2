package org.example;

import org.apache.spark.sql.*;

import java.util.*;
import javax.xml.crypto.Data;
public class SparkProject {
    public static void main(String[] args) {
        try {

            // Reading csv file... type-1
///===========================================================================================================================

//        SparkSession sparkSession = SparkSession.builder().master("local").appName("SPARKDEMO").getOrCreate();
//            Dataset<Row> dataset = sparkSession
//                    .read()
//                    .option("header",true)  // for header of col top
//                    .csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");
//            dataset.show(false);     // for showing full value in columns.....
//
////             type-2
//            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//        Dataset<Row> csv1 = spark.read().csv("E:\\SpringMongoDB\\SparkDemo\\src\\main\\resources");
//        csv1.show();

            // Writing in CSV file....
///===========================================================================================================================

//            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//            Dataset<Row> data = spark.createDataFrame(Arrays.asList(
//                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234")
//            ), UserModel.class);
//
//            data.show();
//            data.write().format("csv").option("delimiter","=")
//                    .option("header", true).mode(SaveMode.Overwrite)
//                    .save("E:/SpringMongoDB/SparkDemo2/src/main/resources/User");

//             updating....
///======================================================================================================================================

//            SparkSession spark = SparkSession.builder().appName("Simple Application")
//                    .master("local").config("spark.driver.host", "localhost").getOrCreate();
//            Dataset<Row> csv1 = spark.read().option("header",true).csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");
//            csv1.show();
//            csv1 = csv1.withColumn("Location",
//                    functions.when(csv1.col("Location").equalTo("Location"),"UK").otherwise(csv1.col("Location")));
//            csv1.show();
//            csv1.write().csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");

///===========================================================================================================================

//            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();
//            Dataset<Row> csv = spark.read().format("csv")
//                    .option("header","true")
//                    .option("delimiter","\t")
//                    .load("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");

            // Describe Schema...

//            csv.printSchema();

            // Selecting Specific Column..

//            csv.select("Location").show();

//            //writing it into another csv file

//            csv.coalesce(1).write().option("header",true).mode(SaveMode.Append).csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile2.csv");


            //filtering data
//            Dataset<Row> filteredData = csv.filter(new Column("Identifier").as("Id").gt(3));
//            filteredData.show();

        // Show the first 3 records in the DataFrame
//            csv.show(3);

// Register the DataFrame as a temporary view
//            csv.createOrReplaceTempView("data");

// Group the data by the 'gender' column and calculate the average 'age'
//            Dataset<Row> grouped = csv.groupBy(col("gender")).agg(avg(col("age")));
// Show the result
//            grouped.show();

//            List<PersonModel> people = Arrays.asList(
//                    new PersonModel("Horvath", 30),
//                    new PersonModel("Brad", 25),
//                    new PersonModel("Jane", 35),
//                    new PersonModel("Jay", 35));

            // Convert the list to a Dataset
//            Dataset<PersonModel> peopleDataset = spark.createDataset(people, Encoders.bean(PersonModel.class));
//
//            peopleDataset.show();

            //selecting columns
//                csv.show();
//            Dataset<Row> selectedCols = csv.select(col("Department,"));
//            selectedCols.show();


            //data from sql
//
//            String url = "jdbc:mysql://localhost:3306/student";
//            String driver = "com.mysql.jdbc.Driver";
//            String user = "root";
//            String password = "admin";

//            Dataset<Row> df =  spark.read()
//                .format("jdbc")
//                .option("driver", driver)
//                .option("url", url)
//                .option("user", user)
//                .option("password", password)
//                .option("dbtable", "students")
//                .load();
//
//            System.out.println("start");
////            df.count();
//            df.show(10);
//            System.out.println("end");
//
//            spark.stop();

///===========================================================================================================================

            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();
//
//            //reading
//            Dataset<Row> data = spark.createDataFrame(Arrays.asList(
//                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234")
//            ), UserModel.class);
//            data.show();

            //writing
//            data.write().option("header",true).mode(SaveMode.Append).csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile2.csv");


            //appending column

//        data = data.withColumn("password",functions.abs(functions.rand()));
//        data.show();

            //drop column

//        data = data.drop("password");
//        data.show();
//        data.write().format("csv").option("header",true).mode(SaveMode.Overwrite).csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile2.csv");

            //changing the data

//        data = data.withColumn("id",functions.when(data.col("id").gt(2),100).otherwise(data.col("id")));
//        data.show();
//        data.write().format("csv").option("header",true).mode(SaveMode.Overwrite).csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile2.csv");

            //distinct values

//        data = data.distinct();
//        data.show();

            //union
//        Dataset<Row> temp = spark.createDataFrame(Arrays.asList(
//                new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234")
//        ), UserModel.class);
//
//        data = data.union(temp);
//        data.show();

            //group by
//        data.groupBy("name").count().show();

            //de duplication
//        Dataset<Row> uniqueData = data.dropDuplicates("id");
//        Dataset<Row> removedData = data.except(uniqueData);
//
//        data.show();
//        uniqueData.show();
//        removedData.show();

                    //.....join.....
            Dataset<Row> leftData = spark.createDataFrame(Arrays.asList(
//                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234")
            ), UserModel.class);
            Dataset<Row> rightData = spark.createDataFrame(Arrays.asList(
                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234"),
                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234")
            ), UserModel.class);

            // Inner join..
//    Dataset<Row> result = leftData.join(rightData, leftData.col("fname").equalTo(rightData.col("fname")),"inner");

            // Left Join...
//    Dataset<Row> result = leftData.join(rightData, leftData.col("fname").equalTo(rightData.col("fname")),"left");

            // Right Join...
//    Dataset<Row> result = leftData.join(rightData, leftData.col("fname").equalTo(rightData.col("fname")),"right");

//        result.show();

            //union
        leftData.union(rightData).show();

///===========================================================================================================================

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}