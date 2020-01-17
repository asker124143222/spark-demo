package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/16 17:09
  * @Version: 1.0
  * @Description: 使用列摘要统计信息将每个特征分别线性地缩放到公共范围[min，max] *，也称为最小-最大规格化或重新缩放
  * 由于零值可能会转换为非零值，因此即使对于稀疏输入，转换器的输出也将是DenseVector
  **/
object Ex_MinMaxScaler {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "features")

    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")

    val model = scaler.fit(dataFrame)

    val scaledData  = model.transform(dataFrame)

    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("features", "scaledFeatures").show()

    spark.stop()
  }
}
