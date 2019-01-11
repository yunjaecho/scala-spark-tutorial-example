package com.sparkTutorial.advanced.broadcast

import java.util.Scanner

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object UkMakerSpaces {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("stackOverFlowSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val postCodeMap = sc.broadcast(loadPostCodeMap())

    val makerSpaceRdd = sc.textFile("in/uk-makerspaces-identifiable-data.csv")
    val regioons = makerSpaceRdd
      .filter(line => {
        line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp"
      })
      .map(line => {
        resutPostPrefix(getPostPrefix(line), postCodeMap)
        /*
        val postPrefix = getPostPrefix(line)
        println("before : " + postPrefix)
        postPrefix match {
          case Some(x) if (postCodeMap.value.contains(x)) => postCodeMap.value.get(x)
          case other => "Unknow"
        }
        println("after : " + postPrefix)
        postPrefix
        */
      })

    regioons.countByValue().foreach(data => println(s"${data._1} : ${data._2}"))
  }

  def resutPostPrefix(postPrefix: Option[String], postCodeMap: Broadcast[Map[String, String]]): String = postPrefix match {
    case Some(x) if (postCodeMap.value.contains(x)) => postCodeMap.value.get(x).get
    case other => "Unknow"
  }


  def getPostPrefix(line: String) = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    if (splits(4).isEmpty) None
    else Some(splits(4).split(" ")(0))
  }


  def loadPostCodeMap() = {
    val postCode = Source
      .fromFile("in/uk-postcode.csv")
      .getLines()
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        (splits(0), splits(7))
    })
    postCode.toMap
  }
}
