package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/3/5 11:17
  * @Version: 1.0
  * @Description: TODO
  **/
object Ex_GaussianMixture {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //    import spark.implicits._

    val data = spark.read.format("libsvm").load("input/sample_kmeans_data.txt")
    data.show(false)

    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(2)
    val model = gmm.fit(data)

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }
    // $example off$

    spark.stop()
  }
}
