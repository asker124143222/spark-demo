package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2020/1/13 10:04
  * @Version: 1.0
  * @Description: TODO
  **/
object Ex_DecisionTree {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //rdd转换成df或者ds需要SparkSession实例的隐式转换
    //导入隐式转换，注意这里的spark不是包名，而是SparkSession的对象名
    import spark.implicits._

    val data = spark.sparkContext.textFile("input/iris.data.txt")
      .map(_.split(","))
      .map(a => Iris(
        Vectors.dense(a(0).toDouble, a(1).toDouble, a(2).toDouble, a(3).toDouble),
        a(4))
      ).toDF()

    data.createOrReplaceTempView("iris")
    val df = spark.sql("select * from iris")
    df.map(r => r(1) + " : " + r(0)).collect().take(10).foreach(println)


    ////对特征列和标签列进行索引转换
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures")
      .setMaxCategories(4).fit(df)


    //决策树分类器
    val dtClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

    //将预测的类别重新转成字符型
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictionLabel").setLabels(labelIndexer.labels)

    //将原数据集拆分成两个部分，一部分用于训练，一部分用于测试
    val Array(trainingData, testData): Array[Dataset[Row]] = df.randomSplit(Array(0.7,0.3))

    //建立工作流
    val pipeline = new Pipeline().setStages(Array(labelIndexer,featureIndexer,dtClassifier,labelConverter))

    //生成训练模型
    val modelDecisionTreeClassifier = pipeline.fit(trainingData)

    //预测
    val result = modelDecisionTreeClassifier.transform(testData)

    result.show(150,false)

    /**
      * 样本分为：正类样本和负类样本。
      * TP：被分类器正确分类的正类样本数。
      * TN: 被分类器正确分类的负类样本数。
      * FP: 被分类器错误分类的正类样本数。（本来是负，被预测为正） ---------->正
      * FN: 被分类器错误分类的负类样本数。 (本来是正， 被预测为负) ---------->负
      *
      * 准确率(Accuracy ACC)
      * 总样本数=TP+TN+FP+FN
      * ACC=(TP+TN)/(总样本数)
      * 该评价指标主要针对分类均匀的数据集。
      */
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy: Double = evaluator.evaluate(result)

    println("Accuracy = " + accuracy)

    /**
      * 精确率（Precision 查准率）
      * Precision = TP / （TP+ FP） 准确率，表示模型预测为正样本的样本中真正为正的比例
      */
    val evaluator2 = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    val weightedPrecision: Double = evaluator2.evaluate(result)

    println("weightedPrecision = " + weightedPrecision)

    /**
      * 召回率（查全率）
      * Recall = TP /（TP + FN） 召回率，表示模型准确预测为正样本的数量占所有正样本数量的比例
      */
    val evaluator3 = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val weightedRecall: Double = evaluator3.evaluate(result)

    println("weightedRecall = " + weightedRecall)


    val treeModel = modelDecisionTreeClassifier.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    //决策树回归器
    val dtRegressor = new DecisionTreeRegressor().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

    val pipelineRegressor = new Pipeline()
      .setStages(Array(labelIndexer,featureIndexer,dtRegressor,labelConverter))

    val modelRegressor = pipelineRegressor.fit(trainingData)
    val result2 = modelRegressor.transform(testData)

    result2.show(150,false)

    //评估
    val regressionEvaluator = new RegressionEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
        .setMetricName("rmse")
    val rmse = regressionEvaluator.evaluate(result2)
    println("rmse = " + rmse)
    spark.stop()
  }
}

case class Iris(features: Vector, label: String)