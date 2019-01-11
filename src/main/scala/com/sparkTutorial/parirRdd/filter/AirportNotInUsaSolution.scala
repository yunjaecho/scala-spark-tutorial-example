package com.sparkTutorial.parirRdd.filter

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportNotInUsaSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val airportsRdd = sc.textFile("in/airports.text")
  val aiprtsNotInUSA =
    airportsRdd.map { line =>
      val splits = line.split(Utils.COMMA_DELIMITER)
      (splits(1), splits(3))
    }.filter { keyValue =>
        keyValue._2 != "\"United States\""
    }

  aiprtsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")
}
