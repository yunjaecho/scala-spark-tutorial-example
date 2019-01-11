package com.sparkTutorial.parirRdd.reducebyeky

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/word_count.text")
  val words = lines.flatMap(_.split(" "))

  val pairWord = words.map(word => (word, 1))
  val reduceWord = pairWord.reduceByKey(_ + _)

  //for ((word, count) <- wordCounts) println(s"$word : $count")
  val wordCount = for ((word, count) <- reduceWord) yield (word, count)
  wordCount.foreach(println)

  println(wordCount)


}
