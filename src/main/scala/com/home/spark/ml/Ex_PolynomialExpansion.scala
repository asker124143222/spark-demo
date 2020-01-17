package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/15 17:29
  * @Version: 1.0
  * @Description: TODO
  * 多项式扩展是将要素扩展到多项式空间的过程，该空间由原始尺寸的n次组合构成。
  * PolynomialExpansion类提供此功能。下面的示例显示如何将特征扩展到3度多项式空间
  * 设置degree为2就可以将(x, y)转化为(x, x x, y, x y, y y)
  **/
object Ex_PolynomialExpansion {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(2.0, 3.0),
      Vectors.dense(3.0, -1.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polynomialExpansion = new PolynomialExpansion().setInputCol("features").setOutputCol("polyFeatures")
      //setDegree表示多项式最高次幂 比如1.0,5.0可以是 三次：1.0^3 5.0^3 1.0+5.0^2 二次：1.0^2+5.0 1.0^2 5.0^2 1.0+5.0 一次：1.0 5.0
      .setDegree(3)
    val result = polynomialExpansion.transform(df)

    result.show(false)


    spark.stop()
  }
}
