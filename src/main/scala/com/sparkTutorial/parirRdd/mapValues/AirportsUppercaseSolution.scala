package com.sparkTutorial.parirRdd.mapValues

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsUppercaseSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val airportsRdd = sc.textFile("in/airports.text")
  val airportPairRDD =
    airportsRdd.map { line =>
      val splits = line.split(Utils.COMMA_DELIMITER)
      (splits(1), splits(3).toUpperCase())
    }//.mapValues {countryName  => countryName.toUpperCase()}

  airportPairRDD.saveAsTextFile("out/airports_uppercase.text")
}
