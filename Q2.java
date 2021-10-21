
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;




public class Main {

    public static void main(String[] args){

        String SPACE_DELIMITER = "\n";

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile1 = sc.textFile("src/main/resources/dataset1.txt");
        JavaRDD<String> textFile2 = sc.textFile("src/main/resources/dataset2.txt");
        JavaRDD<String> textFile3 = textFile1.intersection(textFile2);

        textFile3.flatMap(s -> Arrays.asList(s.split(SPACE_DELIMITER)).iterator());
        textFile3.foreach(p -> System.out.println(p));

        long result = textFile3.count();

        System.out.println("final result is : "+ result );
    }



}
