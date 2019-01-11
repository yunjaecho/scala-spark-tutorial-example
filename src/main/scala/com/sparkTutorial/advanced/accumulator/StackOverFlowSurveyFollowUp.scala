package com.sparkTutorial.advanced.accumulator

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object StackOverFlowSurveyFollowUp extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("stackOverFlowSurvey").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val total = sc.longAccumulator("total")
  val missingSalaryMidPoint = sc.longAccumulator("missing_salary_middle_point")
  val processedBytes = sc.longAccumulator("Processed_bytes")

  val responseFromCanada = sc.textFile("in/2016-stack-overflow-survey-responses.csv")
    .filter(response => {
      val splits = response.split(Utils.COMMA_DELIMITER, -1)

      total.add(1)
      processedBytes.add(response.getBytes().size)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }
      splits(2).equals("Canada")
    })

  println(s"Count of response from Canada count : ${responseFromCanada.count()}")
  println(s"Total count of response : ${total.value}")
  println(s"Number of bytes Processed :  ${processedBytes.value}")
  println(s"Count of response missing salary middle point : ${missingSalaryMidPoint.value}")
}
