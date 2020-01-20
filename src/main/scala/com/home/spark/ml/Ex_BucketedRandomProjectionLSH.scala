package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * @Author: xu.dm
  * @Date: 2020/1/19 10:59
  * @Version: 1.0
  * @Description: BucketedRandomProjectionLSH局部敏感哈希
  * 为欧几里得距离度量实现局部敏感哈希函数
  *
  * 输入是密集或稀疏向量，每个向量代表欧几里得距离空间中的一个点。 输出将是可配置尺寸的向量。 相同维中的哈希值由相同的哈希函数计算。
  **/
object Ex_BucketedRandomProjectionLSH {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 1.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH().setInputCol("features").setOutputCol("hashes")
      //增大参数降低假阴性率，但以增加计算复杂性为代价
      .setNumHashTables(3)
      //每个哈希存储桶的长度（较大的存储桶可降低假阴性）
      .setBucketLength(2.0)

    val model = brp.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show(false)

    // Compute the locality sensitive hashes for the input rows, then perform approximate similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset,
    // e.g. `model.approxSimilarityJoin(transformedA, transformedB, 2.5)`
    println("Approximately joining dfA and dfB on Euclidean distance smaller than 2.5:")
    model.approxSimilarityJoin(dfA, dfB, 2.5, "EuclideanDistance")
      .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("EuclideanDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset,
    // e.g. `model.approxNearestNeighbors(transformedA, key, 2)`
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show(false)

    spark.stop()
  }
}
