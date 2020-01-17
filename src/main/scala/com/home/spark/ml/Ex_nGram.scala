package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/15 13:46
  * @Version: 1.0
  * @Description:
  * 一种特征转换器，将输入的字符串数组转换为n元语法的数组。
  * 输入数组中的空值将被忽略，它将返回一个n-gram数组，其中每个n-gram均由以空格分隔的单词字符串表示。
  * 当输入为空时，将返回一个空数组。 当输入数组的长度小于n（每n-gram的元素数）时，不返回n-gram。
  *
  **/
object Ex_nGram {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val gram = new NGram().setInputCol("words").setOutputCol("nGrams")
      //Default: 2, bigram features
      .setN(2)
    val result = gram.transform(wordDataFrame)


    result.show(false)



    spark.stop()
  }
}
