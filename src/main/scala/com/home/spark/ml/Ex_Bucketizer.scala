package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/17 11:44
  * @Version: 1.0
  * @Description: TODO
  * Bucketizer将一列连续特征转换为一列特征存储桶，其中存储桶由用户指定。它带有一个参数：
  *
  * splits：用于将连续要素映射到存储桶的参数。使用n + 1个拆分，有n个存储桶。
  * 拆分x，y定义的存储区除最后一个存储区（也包含y）外，其值都在[x，y）范围内。
  * 分割数应严格增加。必须明确提供-inf，inf的值以覆盖所有Double值；否则，超出指定分割的值将被视为错误。
  *
  * 拆分的两个示例是Array（Double.NegativeInfinity，0.0，1.0，Double.PositiveInfinity）和Array（0.0，1.0，2.0）。
  *
  * 请注意，如果您不了解目标列的上限和下限，则应添加Double.NegativeInfinity和Double.PositiveInfinity作为拆分的边界，以防止出现超出Bucketizer边界的异常。
  *
  * 注意，您提供的拆分必须严格按升序排列，即s0 <s1 <s2 <... <sn。
  **/
object Ex_Bucketizer {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2,0.5,999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer  = new Bucketizer().setInputCol("features").setOutputCol("bucketedFeatures").setSplits(splits)
    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show()


    //多列特征也需要多列分隔机制
    val splitsArray = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity))

    val data2 = Array(
      (-999.9, -999.9),
      (-0.5, -0.2),
      (-0.3, -0.1),
      (0.0, 0.0),
      (0.2, 0.4),
      (999.9, 999.9))
    val dataFrame2 = spark.createDataFrame(data2).toDF("features1", "features2")

    val bucketizer2 = new Bucketizer()
      .setInputCols(Array("features1", "features2"))
      .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
      .setSplitsArray(splitsArray)

    // Transform original data into its bucket index.
    val bucketedData2 = bucketizer2.transform(dataFrame2)

    println(s"Bucketizer output with [" +
      s"${bucketizer2.getSplitsArray(0).length-1}, " +
      s"${bucketizer2.getSplitsArray(1).length-1}] buckets for each input column")
    bucketedData2.show()

    spark.stop()
  }

}
