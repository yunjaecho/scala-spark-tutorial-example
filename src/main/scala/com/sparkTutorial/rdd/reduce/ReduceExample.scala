package com.sparkTutorial.rdd.reduce

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ReduceExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputInteger = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputInteger)

    val product = integerRdd.reduce(_ + _)
    println(s"product is : $product")
  }

}
