package com.sparkTutorial.parirRdd.aggregation.reducebykey.housePrice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/RealEstate.csv")
  val cleanLines = lines
    .filter(!_.contains("Bedrooms"))

  val housePricePairRdd = cleanLines.map(line => {
    val splits = line.split(",")
    (splits(3), new AvgCount(1, splits(2).toDouble))
  })

  val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => new AvgCount(x.count + y.count, x.total + y.total))
  println("housePriceTotal: ")

  housePriceTotal.foreach(housePrice =>
    println(s"${housePrice._1} : ${housePrice._2}")
  )

  val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount.total/ avgCount.count)
  println("housePriceAvg : ")

  housePriceAvg.foreach(housePrice =>
    println(s"${housePrice._1} : ${housePrice._2}")
  )

}
