package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/3/6 16:11
  * @Version: 1.0
  * @Description: TrainValidationSplit
  *  除CrossValidator外，Spark还提供TrainValidationSplit用于超参数调整。 TrainValidationSplit仅对每个参数组合进行一次评估，
  *  而对于CrossValidator而言，则仅进行k次评估。因此，它便宜些，但是当训练数据集不够大时，不会产生可靠的结果。
  *  与CrossValidator不同，TrainValidationSplit创建单个（训练，测试）数据集对。
  *  它将使用trainRatio参数将数据集分为这两部分。例如，trainRatio = 0.75
  *   ，TrainValidationSplit将生成一个训练和测试数据集对，其中75％的数据用于训练，而25％的数据用于验证。
  *  像CrossValidator一样，TrainValidationSplit最终使用最佳的ParamMap和整个数据集来拟合Estimator。
  **/
object Ex_TrainValidationSplit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark ml model selection").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // Prepare training and test data.
    val data = spark.read.format("libsvm").load("input/sample_linear_regression_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()
      .setMaxIter(10)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)
      // Evaluate up to 2 parameter settings in parallel
      .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    model.transform(test)
      .select("features", "label", "prediction")
      .show()
    spark.stop()
  }
}
