package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/16 14:39
  * @Version: 1.0
  * @Description: 使用给定的p范数对向量进行归一化，使其具有单位范数
  **/
object Ex_Normalizer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures").setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)
    println("Normalized using L^1 norm")
    l1NormData.show(false)


    val l2NormData = normalizer.transform(dataFrame,normalizer.p->2)
    println("Normalized using L^2 norm")
    l2NormData.show(false)


    val linfiData = normalizer.transform(dataFrame,normalizer.p->Double.PositiveInfinity)
    println("Normalized using L^inf norm")
    linfiData.show(false)

    spark.stop()
  }
}
