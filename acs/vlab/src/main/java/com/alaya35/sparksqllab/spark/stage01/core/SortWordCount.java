package com.alaya35.sparksqllab.spark.stage01.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * 排序的wordcount程序
 * Created by gauge on 2017/3/2.
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SparkWordCount")
                .set("spark.ui.port", "44040");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://spark:8020/spark.txt", 1);
        JavaRDD<String> words = lines.flatMap(l -> {
            return Arrays.asList(l.split(" "));
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }

        });

        JavaPairRDD<String, Integer> wordcounts = pairs.reduceByKey((v1, v2) -> {
            return v1 + v2;
        });

// 到这里为止，就得到了每个单词出现的次数
        // 但是，问题是，我们的新需求，是要按照每个单词出现次数的顺序，降序排序
        // wordCounts RDD内的元素是什么？应该是这种格式的吧：(hello, 3) (you, 2)
        // 我们需要将RDD转换成(3, hello) (2, you)的这种格式，才能根据单词出现次数进行排序把！

        // 进行key-value的反转映射
        JavaPairRDD<Integer, String> countwords = wordcounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }

        });

        JavaPairRDD<Integer, String> sortedCountWord = countwords.sortByKey(false);
        JavaPairRDD<String, Integer> sortedWordCount = sortedCountWord.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2, t._1);
            }

        });

        sortedCountWord.saveAsTextFile("hdfs://spark:8020/spark-sortwordcount-result.txt");
        // JavaPairRDD<Integer,String> topCountWord = sortedCountWord.take(50);
        // 到此为止，我们获得了按照单词出现次数排序后的单词计数
        // 打印出来
        sortedCountWord.foreach(t -> {
            System.out.println(t._1 + " appears " + t._2 + " times.");
        });


        sc.close();


    }


}
