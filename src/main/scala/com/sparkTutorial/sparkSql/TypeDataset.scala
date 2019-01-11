package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max}
import org.apache.spark.sql.types.IntegerType

object TypeDataset {
  val AGE_MIDPOINT = "ageMidpoint"
  val SALARY_MIDPOINT = "salaryMidPoint"
  val SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
    val dataFrameReader = session.read
    val response: Dataset[Row] = dataFrameReader.option("header", true).csv("in/2016-stack-overflow-survey-responses.csv")

    val responseWithSelectedColumns: Dataset[Row] = response.select(
      col("country"),
      col("age_midpoint").as("ageMidPoint").cast(IntegerType),
      col("occupation"),
      col("salary_midpoint").as("salaryMidPoint").cast(IntegerType)
    )

    val typedDataset: Dataset[Response] = responseWithSelectedColumns.as(Encoders.bean(classOf[Response]))

    println("=== Print out schema ===")
    typedDataset.printSchema()

    println("=== Print 20 records of response table ===")
    typedDataset.show(20)

    println("=== Print the responses from Afghanistan ===")
    typedDataset.filter(response => response.getCountry.equals("Afghanistan")).show()

    println("=== Print the count of occupations ===")
    typedDataset.groupBy(col("occupation")).count().show()

    println("=== Print response with average mid age less than 20 ===")
    typedDataset.filter(response => response.getAgeMidPoint != null && response.getAgeMidPoint < 20).show()

    println("=== Pinrt the result by salary middle point in descending order ===")
    typedDataset.orderBy(col(SALARY_MIDPOINT).desc).show()


  }

  //class Response(var country: String, var ageMidPoint: Integer, var occupation: String, var salaryMidPoint: Integer) extends Serializable
}


