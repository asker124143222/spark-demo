package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/3/5 9:39
  * @Version: 1.0
  * @Description: TODO
  **/
object Ex_Kmeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    import spark.implicits._

    val data = spark.read.format("libsvm").load("input/sample_kmeans_data.txt")
    data.show(false)


    val kmeans = new KMeans().setK(2).setSeed(1L)

    val model = kmeans.fit(data)

    // Make predictions
    val predictions = model.transform(data)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    spark.stop()
  }

}
