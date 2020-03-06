package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

/**
  * @Author: xu.dm
  * @Date: 2020/3/3 11:20
  * @Version: 1.0
  * @Description:
  * TODO Logistic回归的spark.ml实现还支持提取训练集中的模型摘要。
  * 请注意，作为DataFrame存储在BinaryLogisticRegressionSummary中的预测和度量标有@transient注释，因此仅在驱动程序上可用。
  **/
object LogisticRegressionSummaryExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val training = spark.read.format("libsvm").load("input/sample_libsvm_data.txt")

    //    training.show(20,false)

    val lr = new LogisticRegression().setMaxIter(10)
      .setElasticNetParam(0.8).setRegParam(0.3)

    // Fit the model
    val lrModel = lr.fit(training)

    // $example on$
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val trainingSummary = lrModel.binarySummary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = trainingSummary.roc
    roc.show()
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    // Set the model threshold to maximize F-Measure
    val fMeasure = trainingSummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lrModel.setThreshold(bestThreshold)
    // $example off$


    spark.stop()
  }
}
