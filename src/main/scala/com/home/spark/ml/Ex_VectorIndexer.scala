package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2020/1/14 16:24
  * @Version: 1.0
  * @Description:
  * VectorIndexer帮助索引Vector数据集中的分类特征。它既可以自动确定哪些特征是分类的，又可以将原始值转换为分类索引。
  *
  * 采取类型为Vector的输入列和参数maxCategories。
  * 根据不同值的数量确定应分类的要素，其中最多具有maxCategories的要素被声明为分类。
  * 为每个分类特征计算从0开始的分类索引。
  * 为分类特征建立索引，并将原始特征值转换为索引。
  *
  * 索引分类特征允许诸如决策树和树组合之类的算法适当地处理分类特征，从而提高性能。
  **/
object Ex_VectorIndexer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = spark.read.format("libsvm")
//      .option("numFeatures",1000)
      .load("input/sample_libsvm_data.txt")

    data.show()

    val indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} " +
      s"categorical features: ${categoricalFeatures.mkString(", ")}")

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()

    spark.stop()
  }
}
