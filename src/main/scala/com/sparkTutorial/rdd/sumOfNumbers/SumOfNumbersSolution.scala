package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SumOfNumbersSolution {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/prime_nums.text")
    val numbers = lines
      .flatMap(_.split("\\s+"))
      .filter(!_.isEmpty)
      .map(_.toInt)

    println(s"Sum is : ${numbers.sum()}")

    println(s"Sum is : ${numbers.reduce(_ + _)}")

  }

}
