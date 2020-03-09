package com.home.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: xu.dm
  * @Date: 2020/3/9 11:01
  * @Version: 1.0
  * @Description: 向量统计信息
  **/
object Ex_SummaryStatistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml stat").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val observations = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )

    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.mean)  // a dense vector containing the mean value for each column
    println(summary.variance)  // column-wise variance
    println(summary.numNonzeros)  // number of nonzeros in each column
    println(summary.normL2)
    println(summary.normL1)

    sc.stop()
  }
}
