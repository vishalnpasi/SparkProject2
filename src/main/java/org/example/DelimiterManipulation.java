package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class DelimiterManipulation {
    public static void main(String[] args) {
        try{
//========================  Change Delimiter  =======================================

            String NewDelimiter = "," , OldDelimiter = ",";
            Scanner scanner = new Scanner(System.in);
            System.out.print("\nEnter Your Old delimiter:=>");
            OldDelimiter = scanner.nextLine();
            System.out.print("\nEnter Your New Delimiter:=>");
            NewDelimiter =scanner.nextLine();
            SparkSession sparkSession = SparkSession.builder().master("local").appName("SPARKDEMO").getOrCreate();
            Dataset<Row> dataset = sparkSession.read()
                    .option("delimiter",OldDelimiter)       // if this option isn't mention then it consider comma by default.
                    .option("header",true)  // for header of col top
                    .csv("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");
            dataset.show();
            dataset.write().format("csv")
                    .option("delimiter",NewDelimiter)
                    .option("header", true)
                    .mode(SaveMode.Overwrite)
                    .save("E:/SpringMongoDB/SparkDemo2/src/main/resources/Delimiter");

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
