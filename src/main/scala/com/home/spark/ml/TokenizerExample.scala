package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @Author: xu.dm
  * @Date: 2020/1/3 16:43
  * @Version: 1.0
  * @Description: Tokenizer分词器
  **/
object TokenizerExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName("TokenizerExample")
      .getOrCreate()

    // $example on$
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    //自定义函数
    val countTokens = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)
    // $example off$

    spark.stop()


  }
}
