package com.alaya35.sparksqllab.acs.scalalab

import org.apache.spark.SparkConf
import  org.apache.spark.SparkContext

/**
  * Created by gauge on 2017/2/28.
  */
object AccumulatorVariable {

    def main (args:Array[String]): Unit ={
        val conf = new SparkConf()
                .setAppName("AccumulatorVariable")
                .set("spark.ui.port","44040")
        val sc = new SparkContext(conf)

      val sum = sc.accumulator(0)

      val numberArray = Array(1,2,3,4,5)

      val numbers = sc.parallelize(numberArray,1)

      numbers.foreach(num => sum += num)

      println(sum)


    }


}
