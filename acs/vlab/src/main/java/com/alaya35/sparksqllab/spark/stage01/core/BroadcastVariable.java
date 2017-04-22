package com.alaya35.sparksqllab.spark.stage01.core;

        import java.util.Arrays;
        import java.util.List;

        import org.apache.spark.Accumulable;
        import org.apache.spark.Accumulator;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.api.java.function.Function;
        import org.apache.spark.api.java.function.VoidFunction;
        import org.apache.spark.broadcast.Broadcast;
        import org.apache.spark.sql.catalyst.expressions.In;

/**
 * 广播变量
 * Created by gauge on 2017/2/28.
 */
public class BroadcastVariable {

public  static  void main (String[] args) {
    SparkConf conf = new SparkConf()
            .setAppName("BroadcastVariable")
            .set("spark.ui.port", "44040");
    JavaSparkContext sc = new JavaSparkContext(conf);

    final int factor = 3;

    final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
    final Accumulator<Integer> sum = sc.accumulator(0);

    List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> numbers = sc.parallelize(numberList);

    JavaRDD<Integer> mutipleNumbers = numbers.map(v1 -> {
        //return  v1* factor;
//        int factor = factorBroadcast.value();
        return v1 * factorBroadcast.value();
    });

    mutipleNumbers.foreach(t -> {
        sum.add(t);
        System.out.println(t);

    });
    System.out.println(sum.value());
    sc.close();

}



}





