import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.lang.String;

public class PageRank {
    public static void main(String[] args) throws Exception {
        System.out.println(1 + 3);
        SparkConf conf = new SparkConf()
                .setAppName("Word Count")
                .setMaster(("local[2]"));
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(1 + 3);
        JavaRDD<String> lines = sc.textFile("file://home/emil/hadoop/sphere_hadoop/hw4/data/salam.txt");
        System.out.println(1 + 3);
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
    }
}
