package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/16 16:41
  * @Version: 1.0
  * @Description: 通过删除平均值并使用列摘要对训练集中的样本进行统计，将其缩放为单位方差来标准化功能
  **/
object Ex_StandardScaler {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = spark.read.format("libsvm").load("input/sample_libsvm_data.txt")

    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")
      .setWithStd(true).setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(data)

    // Normalize each feature to have unit standard deviation.
    val result = scalerModel.transform(data)

    result.show()

    spark.stop()
  }

}
