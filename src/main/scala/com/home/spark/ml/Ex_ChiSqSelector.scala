package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/18 16:12
  * @Version: 1.0
  * @Description: 卡方选择器
  * ChiSqSelector代表卡方特征选择。它对具有分类特征的标记数据进行操作。
  * ChiSqSelector使用卡方独立性检验来决定选择哪些功能。
  * 它支持五种选择方法：numTopFeatures，percentile，fpr，fdr，fwe：
  *
  * numTopFeatures根据卡方检验选择固定数量的Top特征。这类似于产生具有最大预测能力的特征。
  * percentile与numTopFeatures类似，但是选择所有功能的一部分而不是固定数量。
  * fpr选择p值低于阈值的所有特征，从而控制选择的误报率。
  * fdr使用Benjamini-Hochberg过程选择错误发现率低于阈值的所有特征。
  * fwe选择p值低于阈值的所有特征。阈值按1 / numFeatures缩放，从而控制选择的家庭式错误率。
  * 默认情况下，选择方法是numTopFeatures，TopFeatures的默认数量设置为50。
  * 可以使用setSelectorType选择选择方法。
  **/
object Ex_ChiSqSelector {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val data = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )

    val df = spark.createDataset(data).toDF("id", "features", "clicked")

    val selector = new ChiSqSelector()
      .setNumTopFeatures(2)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)

    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.show()

    spark.stop()
  }
}
