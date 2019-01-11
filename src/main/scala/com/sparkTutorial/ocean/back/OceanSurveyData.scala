package com.sparkTutorial.ocean.back

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection._

import scala.io.Source

object OceanSurveyData {

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
        val arrayData = data.split(Utils.COMMA_DELIMITER)
        val amtList = arrayData.drop(6)
        val dayAmtList = headerData zip amtList
        OceanSurvey(arrayData(0), arrayData(1), arrayData(2), arrayData(3), arrayData(4), dayAmtList)
      })

    val oceanSurvey = bodyData.flatMap {data =>
      for (dayAmt <- data.dayAmtList) yield {
        OceanSurveyResult(data.saleryType, data.fishKind, data.region, data.storeType, data.sizeType, dayAmt._1, dayAmt._2)
      }
    }

    //oceanSurvey.foreach(println(_))

    val conf = new SparkConf().setAppName("oceanSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    import session.implicits._
    val dataset = sc.parallelize(oceanSurvey).toDS()

    dataset.show()

    dataset
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("out/oceanSurvey.csv")


    /*bodyData.foreach {data =>
      for (i <- 0 to headerData.size - 1) {
        //surveyList += OceanSurvey(data(0), data(1), data(2), data(3), data(4), "2017-" + headerData(i))
        surveyList += data(0) + "," + data(1) + "," + data(2) + "," + data(3) + "," + data(4) + "," + headerData(i) + "," + data(6+i)
      }
    }

    val conf = new SparkConf().setAppName("oceanSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    sc.parallelize(surveyList).saveAsTextFile("out/oceanSurvey.csv")
    */
  }

}


case class OceanSurvey(saleryType: String, fishKind: String, region: String,
                       storeType: String, sizeType: String, dayAmtList: List[(String, String)])


case class OceanSurveyResult(saleryType: String, fishKind: String, region: String,
                             storeType: String, sizeType: String, day: String, amt: String)

