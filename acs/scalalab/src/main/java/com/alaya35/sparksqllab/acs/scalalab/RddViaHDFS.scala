package com.alaya35.sparksqllab.acs.scalalab

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
  * Created by gauge on 2017/2/27.
  */

object RddViaHDFS {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .set("spark.ui.port", "44040");
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark:8020/spark.txt", 1);

    val words = lines.flatMap { line => line.split(" ") }
    //    val words = lines.flatMap { line =>   line }
    val pairs = words.map { word => (word, 1) }

    //val result = pairs.reduceByKey { _ + _  }
    var result = pairs.reduceByKey((x, y) => x + y)

    result.foreach(result => println(result._1 + " appeared " + result._2 + " times."))
    println(sc.appName + " " + sc.applicationId);
    println("总计：result.collect() = " + result.count()) // 低性能

    result.saveAsTextFile("hdfs://spark:8020/spark-result.txt")
  }

}