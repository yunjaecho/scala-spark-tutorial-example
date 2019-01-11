package com.sparkTutorial.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

object WordCount2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/word_count.text")
  val words = lines.flatMap(_.split(" "))

  val wordCounts = words.countByValue()

  //for ((word, count) <- wordCounts) println(s"$word : $count")
  val wordCount: Map[String, Long] = for ((word, count) <- wordCounts) yield (word, count)
  wordCount.foreach(println)

  println("=========================================")
  val wordCounts2 = words.map((_, 1)).reduceByKey(_ + _)
  wordCounts2.foreach(println)
}
