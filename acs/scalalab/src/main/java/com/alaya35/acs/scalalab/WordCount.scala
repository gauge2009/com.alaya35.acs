package com.alaya35.acs.scalalab

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by gauge on 2016/12/24.
  */


object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .set("spark.ui.port", "44040");
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark:8020/sparkdoc2.txt", 1);
//    val words = lines.flatMap { line =>   line.split(" ")  }
    val words = lines.flatMap { line =>   line }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey { _ + _  }

     wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))
    println(sc.appName);
  }

}