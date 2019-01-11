package com.sparkTutorial.ocean

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties
import scala.io.Source
import com.typesafe.config.{Config, ConfigFactory}


object OceanSurveyData3 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //(10 to  16).map("20"+_).foreach(oceanFishAmtAnalyze(_))
    oceanFishAmtAnalyze("2009")

  }

  def oceanFishAmtAnalyze(year:String): Unit = {
    val input = Source
      .fromFile(s"in/oceanFishAmtData_$year.csv")
      .getLines()
      .toList


    val headerMonth = input.take(1).flatMap(data => data.split(Utils.COMMA_DELIMITER)).drop(6)
    val headerDay = input.drop(1).flatMap(data => data.split(Utils.COMMA_DELIMITER)).drop(6)

    val headerData:List[String] = (headerMonth zip headerDay).map(data => s"$year-${data._1}-${data._2}" )

    //val headerData:List[String] = input.take(1).flatMap(data => data.split(Utils.COMMA_DELIMITER)).drop(6).map(s"$year-" + _)
    val bodyData  = input
      .drop(2)
      .map(data => {
        val arrayData = data.split(Utils.COMMA_DELIMITER)
        val amtList = arrayData.drop(6).map(data => data.replaceAll("\"", "").replaceAll(",", ""))
        val dayAmtList = headerData zip amtList
        OceanSurvey(arrayData(0), arrayData(1), arrayData(2), arrayData(3), arrayData(4), dayAmtList)
      })

    val oceanSurvey = bodyData.flatMap {data =>
      for (dayAmt <- data.dayAmtList) yield {
        OceanSurveyResult(data.saleryType, data.fishKind, data.region, data.storeType, data.sizeType, dayAmt._1, dayAmt._2)
      }
    }


    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()
    import session.implicits._
    val dataset = session.createDataset(oceanSurvey)

    dataset
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(s"out/oceanFishAmtAnalyze_$year.csv")

    val config = ConfigFactory.load()
    val url = config.getString("postgres.url")
    val table = config.getString("postgres.table")
    val props: Properties = propsFromConfig(config.getConfig("postgres.properties"))

    dataset.write.mode(SaveMode.Append).jdbc(url, table, props)

    session.stop()
  }


  def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._

    val props = new Properties()

    val map: Map[String, Object] = config.entrySet().map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map)
    props
  }

}


case class OceanSurvey(saleryType: String, fishKind: String, region: String,
                       storeType: String, sizeType: String, dayAmtList: List[(String, String)])


case class OceanSurveyResult(saleryType: String, fishKind: String, region: String,
                             storeType: String, sizeType: String, day: String, amt: String)