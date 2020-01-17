package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/16 10:52
  * @Version: 1.0
  * @Description: Discrete Cosine Transform (DCT)
  * 离散余弦变换将时域中长度为N的实值序列转换为频域中另一个长度为N的实值序列。
  * 离散余弦变换（DCT for Discrete Cosine Transform）是与傅里叶变换相关的一种变换，它与离散傅里叶变换类似，但是只使用实数。
  *
  * 这种变化经常被信号处理和图像处理使用，用于对信号和图像（包括静止图像和运动图像）进行有损压缩。
  * 在压缩算法中，现将输入图像划分为8*8或16*16的图像块，对每个图像块作DCT变换；然后舍弃高频的系数，
  * 并对余下的系数进行量化以进一步减少数据量；最后使用无失真编码来完成压缩任务。解压缩时首先对每个图像块作DCT反变换，然后将图像拼接成一副完整的图像。
  * 大多数自然信号（包括声音和图像）的能量都集中在余弦变换后的低频部分。
  * 由于人眼对于细节信息不是很敏感，因此信息含量更少的高频部分可以直接去掉，从而在后续的压缩操作中获得较高的压缩比。
  **/
object Ex_DCT {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val result = dct.transform(df)
    result.show(false)

    spark.stop()
  }
}
