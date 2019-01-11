package com.sparkTutorial.parirRdd.sort

import com.sparkTutorial.parirRdd.aggregation.reducebykey.housePrice.AvgCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution extends App {
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext(conf)

  val housePriceRdd = sc.textFile("in/RealEstate.csv")
    .filter(!_.contains("Bedrooms"))
    .map(line => {
      val splits = line.split(",")
      (splits(3).toInt, new AvgCount(1, splits(2).toDouble))
    })

  val housePriceTotal = housePriceRdd.reduceByKey((x, y) => (new AvgCount(x.count + y.count, x.total + y.total)))
  val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)
  //val sortedHousePriceAvg = housePriceAvg.sortByKey(true)

  housePriceAvg.foreach {data =>
    println(s"${data._1} : ${data._2}")
  }

  println("===============================")

  housePriceAvg.sortByKey().collect().foreach(println(_))

}
