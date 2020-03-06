package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/3/5 11:09
  * @Version: 1.0
  * @Description: TODO
  **/
object Ex_BisectingKMeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //    import spark.implicits._

    val data = spark.read.format("libsvm").load("input/sample_kmeans_data.txt")
    data.show(false)

    // Trains a bisecting k-means model.
    val bkm = new BisectingKMeans().setK(2).setSeed(1)
    val model = bkm.fit(data)

    // Make predictions
    val predictions = model.transform(data)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)
    // $example off$

    spark.stop()
  }
}
