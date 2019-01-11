package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, avg, max}
import org.apache.spark.sql.types.IntegerType

object StackOverFlowSurvey {
  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val dataFrameReader = session.read

    val response: Dataset[Row] = dataFrameReader.option("header", true).csv("in/2016-stack-overflow-survey-responses.csv")

    println("=== Print out schema ===")
    response.printSchema()

    println("=== Print 20 records of response table ===")
    response.show(20)

    println("=== Print the so region and self identitfication columns of gender table ==")
    response.select("so_region", "self_identification").show

    println("=== Print records where the reponse is from Afghanistan ===")
    //response.filter("country == 'Afghanistan'").show()
    response.filter(col("country").equalTo("Afghanistan")).show()


    //val groupedDataset = response.groupBy("occupation")
    val groupedDataset = response.groupBy(col("occupation"))
    groupedDataset.count().show()

    print("=== Coast the salay mid point and age mid to integer ===")
    val castedResponse = response
      .withColumn(SALARY_MIDPOINT, col(SALARY_MIDPOINT).cast(IntegerType))
      .withColumn(AGE_MIDPOINT, col(AGE_MIDPOINT).cast(IntegerType))
      //.withColumn(SALARY_MIDPOINT, response(SALARY_MIDPOINT).cast("Int"))
      //.withColumn(AGE_MIDPOINT, response(AGE_MIDPOINT).cast("Int"))

    println("=== Print out casted schema ===")
    castedResponse.printSchema()

    println("=== Print records with average mid age less than 20 ===")
    //castedResponse.filter(s"$AGE_MIDPOINT < 20").show()
    castedResponse.filter(col(AGE_MIDPOINT).$less(20)).show()
    castedResponse.filter(col(AGE_MIDPOINT) < 20).show()

    println("=== Print the result by salary middle point in descending order ===")
    castedResponse.orderBy(col(SALARY_MIDPOINT).desc).show()

    println("=== Group  by country and aggregate by average salary middle point and max age middle point ===")
    val datasetGroupbyCountry = castedResponse.groupBy(col("country"))
    datasetGroupbyCountry.agg(avg(SALARY_MIDPOINT), max(AGE_MIDPOINT)).show()

    val responseWithSalaryBucket = castedResponse.withColumn(
      SALARY_MIDPOINT_BUCKET, col(SALARY_MIDPOINT).divide(20000).cast(IntegerType).multiply(20000)
    )

    println("=== With salary bucket column ===")
    responseWithSalaryBucket.select(col(SALARY_MIDPOINT), col(SALARY_MIDPOINT_BUCKET)).show()

    println("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(col(SALARY_MIDPOINT_BUCKET)).show()

    session.stop()
  }
}
