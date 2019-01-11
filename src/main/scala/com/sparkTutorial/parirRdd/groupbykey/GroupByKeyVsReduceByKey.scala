package com.sparkTutorial.parirRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyVsReduceByKey extends App {
  val conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext(conf)

  val words = List("one", "two", "three", "three", "three")
  val wordsPairRddd = sc.parallelize(words).map((_, 1)).reduceByKey(_ + _)

  wordsPairRddd.foreach(word => println(s"${word._1} : ${word._2}"))


}
