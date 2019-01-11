package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsLatitudeSolution {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")
    val airportsInUSA = airports.filter(_.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    val airportsNameAndCity = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportsNameAndCity.saveAsTextFile("out/airports_by_latitude.text")
  }
}
