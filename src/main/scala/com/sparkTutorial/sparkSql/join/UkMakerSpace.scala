package com.sparkTutorial.sparkSql.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit}

object UkMakerSpace {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate()

    val makerSpace: Dataset[Row] = session.read.option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv")
    val postCode: Dataset[Row] = session.read.option("header", "true").csv("in/uk-postcode.csv")
      .withColumn("PostCode", concat_ws("", col("PostCode"), lit(" ")))

    println("=== Print 20 records of maker space table ===")
    makerSpace.show()

    println("=== Print 20 recrods of postcode table ===")
    postCode.show()

    val joined: Dataset[Row] = makerSpace.join(postCode, makerSpace.col("PostCode").startsWith(postCode.col("PostCode")), "left_outer")

    println("=== Group by Region ===")
    joined.groupBy(col("Region")).count().show(200)


  }
}
