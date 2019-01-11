package com.sparkTutorial.advanced.accumulator

import com.sparkTutorial.commons.Utils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}

object StackOverFlowSurvey extends App {
  val conf = new SparkConf().setAppName("StackOverFlow").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val total = sc.longAccumulator("total")
  val missingSalaryMidPoint = sc.longAccumulator("missing_salary_middle_point")

  val lists = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

  val responseFromCanada = lists.filter(response => {
    val splits = response.split(Utils.COMMA_DELIMITER, -1)
    total.add(1)

    if (splits(14).isEmpty) {
      missingSalaryMidPoint.add(1)
    }
    splits(2).equals("Canada")
  })

  println(s"Count of response from Canada count : ${responseFromCanada.count()}")
  println(s"Total count of response : ${total.value}")
  println(s"Count of response missing salary middle point : ${missingSalaryMidPoint.count}")


}
