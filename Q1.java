
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
        System.out.println("please enter word: ");
        Scanner in = new Scanner (System.in);
        String input=in.next();
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile = sc.textFile("src/main/resources/dataset1.txt");

        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(SPACE_DELIMITER)).iterator())
                .filter(string -> string.contains(input) )
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total words: " + counts.count());
    }



}
