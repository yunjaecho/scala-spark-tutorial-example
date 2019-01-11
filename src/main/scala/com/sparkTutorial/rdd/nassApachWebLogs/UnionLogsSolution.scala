package com.sparkTutorial.rdd.nassApachWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object UnionLogsSolution {

  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.startsWith("bytes"))

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val aggregateLogLines = julyFirstLogs.union(augustFirstLogs)

    val cleanLogLine = aggregateLogLines.filter(line => isNotHeader(line))

    val sample = cleanLogLine.sample(withReplacement = true, fraction = 0.1)

    sample.saveAsTextFile("out/sample_nass_log.text")

  }
}
