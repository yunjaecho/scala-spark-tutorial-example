package com.sparkTutorial.parirRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountrySolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/airports.text")
  val airportCountry = lines.map {line =>
    val splits = line.split(Utils.COMMA_DELIMITER)
    (splits(3), splits(1))
  }.groupByKey()

  for ((key, value) <- airportCountry) {
    println(s"$key : $value")
  }

}
