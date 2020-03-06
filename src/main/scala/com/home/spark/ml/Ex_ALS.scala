package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/3/5 14:58
  * @Version: 1.0
  * @Description: 协同过滤
  * todo 协作过滤通常用于推荐系统。这些技术旨在填充用户项关联矩阵的缺失条目。
  *   spark.ml当前支持基于模型的协作过滤，其中通过一小部分潜在因素来描述用户和产品，这些潜在因素可用于预测缺少的条目。
  *   spark.ml使用交替最小二乘（ALS）算法来学习这些潜在因素
  *
  *  基于矩阵分解的协作过滤的标准方法将用户项矩阵中的条目视为用户对商品的明确偏好，例如，用户给电影评分。
  *  在许多实际用例中，通常只能访问隐式反馈（例如，视图，点击，购买，喜欢，分享等）。
  *  spark.ml中用于处理此类数据的方法来自隐式反馈数据集的协作过滤。
  *  从本质上讲，此方法不是尝试直接对评分矩阵建模，而是将数据视为代表用户操作观察力的数字（例如，点击次数或某人观看电影的累积时间）。
  *  然后，这些数字与观察到的用户偏好的置信度有关，而不是与对商品的明确评分有关。
  *  然后，该模型尝试查找可用于预测用户对某项商品的期望偏好的潜在因素。
  **/
object Ex_ALS {
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._


    val ratings = spark.read.textFile("input/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      //如果评分矩阵是从其他信息源中得出的（即是从其他信号中推断得出的），则可以将implicitPrefs设置为true以获得更好的结果
//      .setImplicitPrefs(true)
      .setRank(30)
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)

    // Generate top 10 movie recommendations for a specified set of users
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    // Generate top 10 user recommendations for a specified set of movies
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)

    userRecs.show(false)
    movieRecs.show(false)
    userSubsetRecs.show(false)
    movieSubSetRecs.show(false)

    val oneUser = model.recommendForUserSubset(ratings.select("userId").where("userId = 28"), 10)
    oneUser.show(false)
    spark.stop()
  }
}
