package org.example;

import com.opencsv.CSVReader;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class SparkProject {
    private static void parseData(String str) {
        String acctFrom, acctTo, amount;
        Scanner lineScanner = new Scanner(str);
        lineScanner.useDelimiter("|");
        while (lineScanner.hasNext()) {
            acctFrom = lineScanner.next();
            acctTo = lineScanner.next();
            amount = lineScanner.next();
            System.out.println("Account From- " + acctFrom + " Account To- " + acctTo +
                    " Amount- " + amount);
        }
        lineScanner.close();
    }

    public static void main(String[] args) {
        Scanner sc = null;
        try {

            String str = "\"Name\":\"jon\" \"location\":\"3333 abc street\" \"country\":\"usa\"";

// prepare String for CSV parsing
            CSVReader reader = new CSVReader(new FileReader("E:\\SpringMongoDB\\SparkDemo2\\src\\main\\resources\\csvFile.csv"));
            System.out.println(reader.getLinesRead());
//                    CsvReader reader = CsvReader.parse(str.replaceAll("\" *: *\"", ":"));
//                    reader.setDelimiter(' '); // use space a delimiter
//                    reader.readRecord(); // read CSV record
//                    for (int i=0; i<reader.getColumnCount(); i++) // loop thru columns
//                        System.out.printf("Scol[%d]: [%s]%n", i, reader.get(i));

//                    sc = new Scanner(new File("E:\\SpringMongoDB\\SparkDemo2\\src\\main\\resources\\csvFile.csv"));
//                    System.out.println("enter");
//                    // Check if there is another line of input
//                    while(sc.hasNextLine()){
//                        String str = sc.nextLine();
//                        // parse each line using delimiter
//                        parseData(str);
//                    }

//                    FileInputStream fileInputStream=new FileInputStream("E:\\SpringMongoDB\\SparkDemo2\\src\\main\\resources\\csvFile.csv");
//
//                    String s1=new String(fileInputStream.readAllBytes());
//
//                    String[] lineArray=s1.split("\n");
//
//                    List<String[]> separatedValues=new ArrayList<>();
//
//                    for (String line: lineArray) {
//                        separatedValues.add(line.split("\\|"));
//                        System.out.println(separatedValues+" iam s1");
//
//                    }
//                    for (String[]  s: separatedValues) {
//                        for (String s2:s ) {
//                            System.out.print(s2+" ");
//                        }
//                        System.out.println("");
//                    }
//                    fileInputStream.close();
//
//                } catch (IOException exp) {
//                    // TODO Auto-generated catch block
//                    exp.printStackTrace();
//                }finally{
//                    if(sc != null)
//                        sc.close();
//                }


//            String fileLocation = "E:\\SpringMongoDB\\SparkDemo2\\src\\main\\resources\\csvFile.csv";
//
//            File file = new File(fileLocation);
//
//            Scanner scanner = new Scanner(file);
//
//            String newStr[] = scanner.nextLine().split(",");
//
//            String str ="";
////            System.out.println(newStr);
//            Dataset<Row> data;
//
//            for (int i =0;i<newStr.length;i++) {
//
//                str += newStr[i];
//                if (i < newStr.length - 1)
//                    str += '/';
//            }
//                System.out.println(str);
//
//            System.out.println(" hi"+scanner.nextLine());

//            System.setProperty("hadoop.home.dir","C:\\hadoop" );

            // Reading csv file... type-1

//        SparkSession sparkSession = SparkSession.builder().master("local").appName("SPARKDEMO").getOrCreate();
////        String filePath = Main.class.getResource("csvFile.csv").getPath();
//        Dataset<Row> dataset = sparkSession.sqlContext()
//                .read()
//                .format("com.databricks.spark.csv")
//                .option("header",true)  // for header of col top
//                .load("E:/SpringMongoDB/SparkDemo2/src/main/resources/csvFile.csv");
//
//        List<Row> list = dataset.collectAsList();
//        dataset.foreach((ForeachFunction<Row>) row -> System.out.println(row));
//            System.out.println(list);
//            System.out.println(dataset);
//        dataset.show(false);     // for showing full value in columns.....

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
//            data.show();
//            data.write().format("csv").option("header", true).mode(SaveMode.Overwrite).save("E:/SpringMongoDB/SparkDemo2/src/main/resources/User");
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);

        }
    }
}