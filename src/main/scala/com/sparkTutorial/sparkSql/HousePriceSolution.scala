package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, col, max}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object HousePriceSolution {
  val PRICE = "Price"
  val PRICE_SQ_FT = "Price SQ Ft"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

    val realeState: Dataset[Row] = session.read.option("header", "true").csv("in/RealEstate.csv")

    val castedRealEstate: Dataset[Row] = realeState
      .withColumn(PRICE, col(PRICE).cast(LongType))
      .withColumn(PRICE_SQ_FT, col(PRICE_SQ_FT).cast(LongType))

    castedRealEstate.groupBy(col("Location"))
      .agg(avg(PRICE_SQ_FT), max(PRICE))
      .orderBy(col(s"avg($PRICE_SQ_FT)").desc)
      .show()


  }
}
