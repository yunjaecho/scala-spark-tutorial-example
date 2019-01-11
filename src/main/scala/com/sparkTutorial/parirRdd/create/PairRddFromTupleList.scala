package com.sparkTutorial.parirRdd.create

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromTupleList extends App {
  val conf = new SparkConf().setAppName("create").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val tuples = List(("Lily", 23),
                    ("Jack", 29),
                    ("Mary", 29),
                    ("James", 8))

  val pairRdd = sc.parallelize(tuples)
  pairRdd.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")
}
