package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/15 16:02
  * @Version: 1.0
  * @Description:
  * 二进制化是将数字特征阈值化为二进制（0/1）特征的过程。
  * Binarizer采用公共参数inputCol和outputCol以及二进制化的阈值。
  * 大于阈值的特征值将二值化为1.0；等于或小于阈值的值二值化为0.0。
  * InputCol支持Vector和Double类型。
  **/
object Ex_Binarizer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = Array((0, 20.0), (1, 30.0), (2, 55.0),(3, 65.0),(4, 50.0))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

    val binarizer = new Binarizer().setInputCol("feature").setOutputCol("binarized_features").setThreshold(50.0)

    val result = binarizer.transform(dataFrame)

    result.show(false)

    spark.stop()
  }

}
