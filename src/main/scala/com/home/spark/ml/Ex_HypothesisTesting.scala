package com.home.spark.ml

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: xu.dm
  * @Date: 2020/3/9 13:53
  * @Version: 1.0
  * @Description: Hypothesis testing
  *假设检验是一种强大的统计工具，可用来确定结果是否具有统计意义，以及该结果是否偶然发生。
  * spark.mllib当前支持Pearson的卡方（χ2）测试适合性和独立性。
  * 输入数据类型确定是否进行拟合优度或独立性测试。拟合优度检验需要向量的输入类型，而独立性检验则需要矩阵作为输入。
  * spark.mllib还支持输入类型RDD [LabeledPoint]，以通过卡方独立性测试启用功能选择。
  **/
object Ex_HypothesisTesting {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml stat").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // a vector composed of the frequencies of events
    val vec: Vector = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25)

    // compute the goodness of fit. If a second vector to test against is not supplied
    // as a parameter, the test runs against a uniform distribution.
    val goodnessOfFitTestResult = Statistics.chiSqTest(vec)
    // summary of the test including the p-value, degrees of freedom, test statistic, the method
    // used, and the null hypothesis.
    println(s"$goodnessOfFitTestResult\n")

    // a contingency matrix. Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val mat: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    // conduct Pearson's independence test on the input contingency matrix
    val independenceTestResult = Statistics.chiSqTest(mat)
    // summary of the test including the p-value, degrees of freedom
    println(s"$independenceTestResult\n")

    val obs: RDD[LabeledPoint] =
      sc.parallelize(
        Seq(
          LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
          LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 0.0)),
          LabeledPoint(-1.0, Vectors.dense(-1.0, 0.0, -0.5)
          )
        )
      ) // (label, feature) pairs.

    // The contingency table is constructed from the raw (label, feature) pairs and used to conduct
    // the independence test. Returns an array containing the ChiSquaredTestResult for every feature
    // against the label.
    val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
    featureTestResults.zipWithIndex.foreach { case (k, v) =>
      println(s"Column ${(v + 1)} :")
      println(k)
    }  // summary of the test

    sc.stop()
  }
}
