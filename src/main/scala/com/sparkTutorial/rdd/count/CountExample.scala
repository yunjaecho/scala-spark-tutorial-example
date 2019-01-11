package com.sparkTutorial.rdd.count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CountExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "hiv", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)

    val wordCountByValue = wordRdd.countByValue()
    println("countByValue")

    for ((word, count) <- wordCountByValue) println(s"$word : $count")

  }

}
