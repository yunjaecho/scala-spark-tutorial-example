package com.sparkTutorial.rdd.nassApachWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object SampleHostsSolution {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sampleHosts").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile("in/nasa_19950701.tsv").map(_.split("\t")(0))
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv").map(_.split("\t")(0))

    val intersection = julyFirstLogs.intersection(augustFirstLogs).filter(_ != "host")
    intersection.saveAsTextFile("out/nass_logs_same_hots.csv")



  }
}
