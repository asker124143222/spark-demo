package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/18 11:46
  * @Version: 1.0
  * @Description: 实验性的功能
  * 用于使用缺失值所在列的平均值或中位数来完成缺失值的归因估算器。 输入列应为DoubleType或FloatType。
  * 当前，Imputer不支持分类功能（SPARK-15041），并且可能为分类功能创建了错误的值。
  * 注意，均值/中值是在滤除缺失值之后计算的。
  * 输入列中的所有Null值都被视为丢失，因此也会被估算。 为了计算中位数，使用DataFrameStatFunctions.approxQuantile的相对误差为0.001。
  **/
object Ex_Imputer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.createDataFrame(Seq(
      (1.0, Double.NaN),
      (2.0, Double.NaN),
      (Double.NaN, 3.0),
      (4.0, 4.0),
      (5.0, 5.0)
    )).toDF("a", "b")

    val imputer = new Imputer()
      .setInputCols(Array("a", "b"))
      .setOutputCols(Array("out_a", "out_b"))
      .setMissingValue(Double.NaN)

    val model = imputer.fit(df)
    model.transform(df).show()

    spark.stop()
  }
}
