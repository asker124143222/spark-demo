package com.home.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ex_checkpoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark checkpoint demo : ").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //需要设定checkpoint持久化目录
    sc.setCheckpointDir("checkpoint")

    val rddList = sc.parallelize(List((-100, "a"), (0, "err"), (1, "a"), (2, "b"), (3, "c"), (4, "b"), (5, "a"), (6, "c")),2)

    val toggleRDD: RDD[(String, Int)] = rddList.map(x=>(x._2,x._1))

    val reduceRDD: RDD[(String, Int)] = toggleRDD.reduceByKey(_+_)

    reduceRDD.checkpoint()

    reduceRDD.collect().foreach(println)

    //打印lineage，使用checkpoint后有明显变化
    println(reduceRDD.toDebugString)

    sc.stop()


  }
}
