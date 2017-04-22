package com.alaya35.sparksqllab.sparksqllabv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by gauge on 2017/4/21.
 */
public class DataFrameCreate {
     public  static void main(String[] args){
         SparkConf conf = new SparkConf()
                 .setAppName("DataFrameCreate");
         JavaSparkContext sc = new JavaSparkContext(conf);
         SQLContext sqlCtx = new SQLContext(sc);
         DataFrame df = sqlCtx.read().json("hdfs://spark:8020/students.json");
        df .show();

     }

}
