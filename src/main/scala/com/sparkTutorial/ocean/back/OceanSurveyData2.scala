package com.sparkTutorial.ocean.back

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object OceanSurveyData2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val input = Source
      .fromFile("in/2017_data.csv")
      .getLines()
      .toList

    //val surveyList = scala.collection.mutable.ListBuffer.empty[OceanSurvey]
    val surveyList = scala.collection.mutable.ListBuffer.empty[String]

    val headerData:List[String] = input.take(1).flatMap(data => data.split(Utils.COMMA_DELIMITER)).drop(6).map("2017-" + _)
    val bodyData  = input
      .drop(1)
      .map(data => {
        data.split(Utils.COMMA_DELIMITER)
      })

    //oceanSurvey.foreach(println(_))

    bodyData.foreach {data =>
      for (i <- 0 to headerData.size - 1) {
        surveyList += data(0) + "," + data(1) + "," + data(2) + "," + data(3) + "," + data(4) + "," + headerData(i) + "," + data(6+i)
      }
    }

    val conf = new SparkConf().setAppName("oceanSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    sc.parallelize(surveyList).saveAsTextFile("out/oceanSurvey2.csv")

  }
}
