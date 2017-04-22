package com.alaya35.sparksqllab


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by gauge on 2017/4/22.
  */
object DataFrameOperation {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("DataFrameCreate")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("hdfs://spark:8020/students.json")

    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 18).show()
    df.groupBy("age").count().show()
  }

}