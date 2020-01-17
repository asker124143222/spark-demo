package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.MaxAbsScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/17 11:00
  * @Version: 1.0
  * @Description: 绝对值最大标准化：MaxAbsScaler
  * 同样是对某一个特征操作，各特征值除以最大绝对值，因此缩放到[-1,1]之间。且不移动中心点。不会将稀疏矩阵变得稠密。
  * 例如一个叫长度的特征，有三个样本有此特征，特征向量为[-1000,100,10],最大绝对值为1000，
  * 转换为[-1000/1000,100/100,10/1000]=[-1,0.1,0.01]。
  *
  * 如果最大绝对值是一个离群点，显然这种处理方式是很不合理的
  **/
object Ex_MaxAbsScaler {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -8.0)),
      (1, Vectors.dense(2.0, 1.0, -4.0)),
      (2, Vectors.dense(4.0, 10.0, 8.0))
    )).toDF("id", "features")

    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MaxAbsScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [-1, 1]
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.select("*").show()

    spark.stop()
  }
}
