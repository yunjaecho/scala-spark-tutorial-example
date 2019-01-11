package com.sparkTutorial.parirRdd.filter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CountExcample extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("count").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
  val wordRdd = sc.parallelize(inputWords)

  println(s"Count : ${wordRdd.count()}")

  val wordCountByValue = wordRdd.countByValue()

  println("CountByValue")

  wordCountByValue.foreach (value =>
    println(value._1 + " : " + value._2)
  )
}
