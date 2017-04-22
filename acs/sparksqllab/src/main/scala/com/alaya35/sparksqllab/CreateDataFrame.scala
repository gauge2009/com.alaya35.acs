

package com.alaya35.sparksqllab


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by gauge on 2017/4/21.
  */
object CreateDataFrame {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("DataFrameCreate")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("hdfs://spark:8020/students.json")

    df.show()
  }

}