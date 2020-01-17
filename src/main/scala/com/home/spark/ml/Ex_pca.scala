package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/15 16:30
  * @Version: 1.0
  * @Description: TODO
  * PCA是一种统计过程，它使用正交变换将一组可能相关的变量的观测值转换为一组线性不相关的变量值（称为主成分）。
  * PCA类训练模型以使用PCA将向量投影到低维空间。
  * 下面的示例显示了如何将5维特征向量投影到3维主成分中。
  **/
object Ex_pca {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures")
      .setK(3)
    val model = pca.fit(df)
    val result = model.transform(df)

    result.show(false)
    spark.stop()
  }
}
