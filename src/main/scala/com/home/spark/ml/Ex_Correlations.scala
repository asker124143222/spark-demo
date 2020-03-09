package com.home.spark.ml

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: xu.dm
  * @Date: 2020/3/9 11:38
  * @Version: 1.0
  * @Description: 相关性
  * 计算两个系列数据之间的相关性是“统计”中的常见操作。在
  * spark.mllib中，我们提供了灵活性来计算许多序列之间的成对相关性。
  * 目前支持的关联方法是Pearson和Spearman的关联
  **/
object Ex_Correlations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml stat").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series
    // must have the same number of partitions and cardinality as seriesX
    val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
    // method is not specified, Pearson's method will be used by default.
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println(s"Correlation is: $correlation")

    val data: RDD[Vector] = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(5.0, 33.0, 366.0))
    )  // note that each Vector is a row and not a column

    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
    // If a method is not specified, Pearson's method will be used by default.
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println(correlMatrix.toString)
    sc.stop()
  }
}
