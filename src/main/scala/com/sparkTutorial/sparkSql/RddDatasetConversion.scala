package com.sparkTutorial.sparkSql

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RddDatasetConversion {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val lines = sc.textFile("in/2016-stack-overflow-survey-responses.csv")
    val responseRDD = lines.filter(line => {
      !(line.split(Utils.COMMA_DELIMITER, -1)(2) == "country")
    }).map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      new Response(splits(2), toInt(splits(6)), splits(9), toInt(splits(14)))
    })

    //import session.implicits
    val responseDataset = session.createDataset(responseRDD)(Encoders.bean(classOf[Response]))

    println("=== Print out schema ===")
    responseDataset.show()

    val responseJavaRdd = responseDataset.toJavaRDD.collect()
    println(responseJavaRdd)
    //responseJavaRdd.forEach(println(_))
  }


  def toInt(split: String): Integer = {
    if (split.isEmpty) null
    else math.round(split.toFloat)
  }

}
