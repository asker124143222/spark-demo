package com.home.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ex_RDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("RDD_Test").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val arrayRDD: RDD[String] = sc.parallelize(Array("a","b","c"))

    println(listRDD)
    listRDD.collect().foreach(println)

    println(arrayRDD)
    arrayRDD.collect().foreach(println)


    val fileRDD: RDD[String] = sc.textFile("input")
    println(fileRDD)
    fileRDD.collect().foreach(println)

//    listRDD.saveAsTextFile("output")
    fileRDD.saveAsTextFile("output")

    sc.stop()

  }

}
