package org.example;

//import com.opencsv.CSVReader;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.ForeachFunction;
//import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

//import java.io.*;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
import java.util.Scanner;

//import com.mongodb.spark.MongoSpark;
//import org.apache.spark.sql.SparkSession;
//import org.bson.Document;
//import static java.util.Arrays.asList;


public class SparkProject {
    public static void main(String[] args) {
        try {

//============  Change Delimiter  =======================================

            String from = "," , to = ",";
            Scanner scanner = new Scanner(System.in);
            System.out.print("\nEnter Your Old delimiter:=>");
            from = scanner.nextLine();
            System.out.print("\nEnter Your New Delimiter:=>");
            to =scanner.nextLine();
            SparkSession sparkSession = SparkSession.builder().master("local").appName("SPARKDEMO").getOrCreate();
            Dataset<Row> dataset = sparkSession.read()
                    .option("delimiter",from)       // if this option isn't mention then it consider comma by default.
                    .option("header",true)  // for header of col top
                    .csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");
            dataset.show();
            dataset.write().format("csv")
                    .option("delimiter",to)
                    .option("header", true).mode(SaveMode.Overwrite)
                    .save("E:/SpringMongoDB/SparkDemo2/src/main/resources/Delimiter");

//=================[ MongoDb ]==========================

//            SparkSession spark = SparkSession.builder()
//                    .master("local")
//                    .appName("MongoSparkConnectorIntro")
//                    .config("spark.mongodb.input.uri", "mongodb+srv://vishalpasi:FbiA1ChEDTbvv6eL@cluster0.3xmrakz.mongodb.net/myCollection")
//                    .config("spark.mongodb.output.uri", "mongodb+srv://vishalpasi:FbiA1ChEDTbvv6eL@cluster0.3xmrakz.mongodb.net/myCollection")
//                    .getOrCreate();
//            // Create a JavaSparkContext using the SparkSession's SparkContext object
//            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//            // More application logic would go here...
//
//            // Create a RDD of 10 documents
//            JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
//                    (new Function<Integer, Document>() {
//                        public Document call(final Integer i) throws Exception {
//                            return Document.parse("{test: " + i + "}");
//                        }
//                    });
//            /*Start Example: Save data from RDD to MongoDB*****************/
//            MongoSpark.save(sparkDocuments, writeConfig);
//            /*End Example**************************************************/
//            jsc.close();

            // Reading csv file... type-1
///===========================================================================================================================

//        SparkSession sparkSession = SparkSession.builder().master("local").appName("SPARKDEMO").getOrCreate();
//            Dataset<Row> dataset = sparkSession
//                    .read()
//                    .option("header",true)  // for header of col top
//                    .csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");
//            dataset.show(false);     // for showing full value in columns.....

            // type-2
//            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//        Dataset<Row> csv1 = spark.read().csv("E:\\SpringMongoDB\\SparkDemo\\src\\main\\resources");
//        csv1.show();

            // Writing in CSV file....

//            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//            Dataset<Row> data = spark.createDataFrame(Arrays.asList(
//                    new UserModel("1", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("2", "vishal", "pasi", "vishal@gmail", "1234"),
//                    new UserModel("3", "vishal", "pasi", "vishal@gmail", "1234")
//            ), UserModel.class);
//
//            data.show();
//            data.write().format("csv").option("delimiter","=").option("header", true).mode(SaveMode.Overwrite).save("E:/SpringMongoDB/SparkDemo2/src/main/resources/User");

            // updating....
///===========================================================================================================================
//            SparkSession spark = SparkSession.builder().appName("Simple Application")
//                    .master("local").config("spark.driver.host", "localhost").getOrCreate();
//            Dataset<Row> csv1 = spark.read().option("header",true).csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");
//            csv1.show();
//            csv1 = csv1.withColumn("Location",
//                    functions.when(csv1.col("Location").equalTo("Location"),"UK").otherwise(csv1.col("Location")));
//            csv1.show();
//            csv1.write().csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");

///===========================================================================================================================

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}