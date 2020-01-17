package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/14 11:33
  * @Version: 1.0
  * @Description: 独热编码（One-Hot Encoding） 
  * 将表示为标签索引的分类特征映射到二进制向量，该向量最多具有一个单一的单值，该单值表示所有特征值集合中特定特征值的存在。
  * 此编码允许期望连续特征（例如逻辑回归）的算法使用分类特征。
  * 对于字符串类型的输入数据，通常首先使用StringIndexer对分类特征进行编码
  *
  * OneHotEncoderEstimator可以转换多列，为每个输入列返回一个热编码的输出矢量列。通常使用VectorAssembler将这些向量合并为单个特征向量。
  *
  * OneHotEncoderEstimator支持handleInvalid参数，以选择在转换数据期间如何处理无效输入。
  * 可用的选项包括“keep”（将任何无效输入分配给额外的分类索引）和“error”（引发错误）。
  **/
object Ex_oneHotEncoder {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0),
      (6.0, 1.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))
      //默认情况下不包括最后一个类别（可通过“dropast”配置），因为它使向量项的总和为1，因此线性相关。
      .setDropLast(false)
    val model = encoder.fit(df)
    
    val encoded = model.transform(df)

    encoded.show(false)

    spark.stop()
  }
}
