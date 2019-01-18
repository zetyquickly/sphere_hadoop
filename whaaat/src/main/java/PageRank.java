import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;
import java.lang.String;

public class PageRank {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("Word Count")
                .setMaster(("local[3]"));
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(1 + 3);
        JavaRDD<String> lines = sc.textFile("file:///home/emil/hadoop/sphere_hadoop/whaaat/data/salam.txt");
        // Split up into words.
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                });
        // Transform into word and count.
        JavaPairRDD<String, Integer> counts = words
                .mapToPair(
                        new PairFunction<String, String, Integer>() {
                            public Tuple2<String, Integer> call(String x) {
                                return new Tuple2(x, 1);
                            }
                        })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer x, Integer y) {
                        return x + y;
                    }
                });
        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile("file:///home/emil/hadoop/sphere_hadoop/whaaat/data/out");
    }
}

