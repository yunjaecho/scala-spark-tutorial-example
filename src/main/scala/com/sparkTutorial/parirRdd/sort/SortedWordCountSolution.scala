package com.sparkTutorial.parirRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SortedWordCountSolution extends App {
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext(conf)

  val wordsRdd = sc.textFile("in/word_count.text")
    .flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKey(_ + _)
    .map(wortToCount => (wortToCount._2, wortToCount._1))
    .sortByKey(false)
    .map(wortToCount => (wortToCount._2, wortToCount._1))

  wordsRdd.foreach {data =>
    println(s"${data._1} : ${data._2}")
  }

}
