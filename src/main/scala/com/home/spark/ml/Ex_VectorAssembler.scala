package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/17 17:02
  * @Version: 1.0
  * @Description:
  * VectorAssembler是一种转换器，它将给定的多个列组合为单个向量列。
  * 这对于将原始特征和由不同特征转换器生成的特征组合到单个特征向量中很有用，以便训练诸如逻辑回归和决策树之类的ML模型。
  *
  * VectorAssembler接受以下输入列类型：所有数字类型，布尔类型和向量类型。在每一行中，输入列的值将按指定顺序连接到向量中。
  **/
object Ex_VectorAssembler {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    val dataset = spark.createDataFrame(
//      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
//    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")


    val dataset = spark.createDataFrame(
      Seq(
        (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
        (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("*").show(false)

    spark.stop()
  }
}
