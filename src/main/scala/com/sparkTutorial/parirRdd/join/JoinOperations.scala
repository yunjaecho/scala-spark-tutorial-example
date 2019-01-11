package com.sparkTutorial.parirRdd.join

import org.apache.spark.{SparkConf, SparkContext}

object JoinOperations extends App {
  val conf = new SparkConf().setAppName("joinOperation").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val ages = sc.parallelize(List(("Tom", 29), ("John", 22)))
  val address = sc.parallelize(List(("James", "USA"), ("John", "UK")))

  val join = ages.join(address)
  join.saveAsTextFile("out/age_address_join.text")

  val leftOuterJoin = ages.leftOuterJoin(address)
  leftOuterJoin.saveAsTextFile("out/age_address_left_out_join.text")

  val rightOuterJoin = ages.rightOuterJoin(address)
  rightOuterJoin.saveAsTextFile("out/age_address_right_join.text")

  val fullOuterJoin = ages.fullOuterJoin(address)
  fullOuterJoin.saveAsTextFile("out/age_addresss_full_join.text")

}
