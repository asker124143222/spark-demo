package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2020/1/8 15:38
  * @Version: 1.0
  * @Description: 逻辑回归，二项分类预测
  *
  **/
object Ex_BinomialLogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark ml label")
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
    data.show()

    data.createOrReplaceTempView("iris")

    val TotalCount = spark.sql("select count(*) from iris")
    println("记录数： " + TotalCount.collect().take(1).mkString)

    //二项预测，由于样本数据有三类数据，排除Iris-setosa
    val df = spark.sql("select * from iris where label!='Iris-setosa'")
    df.map(r => r(1) + " : " + r(0)).collect().take(10).foreach(println)
    println("过滤后的记录数： " + df.count())


    /* VectorIndexer
    提高决策树或随机森林等ML方法的分类效果。
    VectorIndexer是对数据集特征向量中的类别（离散值）特征（index categorical features categorical features ）进行编号。
    它能够自动判断那些特征是离散值型的特征，并对他们进行编号，
    具体做法是通过设置一个maxCategories，特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。
    某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）
    假设maxCategories=5，那么特征列中非重复取值小于等于5的列将被重新索引
    为了索引的稳定性，规定如果这个特征值为0，则一定会被编号成0，这样可以保证向量的稀疏度
    maxCategories缺省是20
    */
    //对特征列和标签列进行索引转换
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer()
//      .setMaxCategories(5) //设置为5后，由于特征列的非重复值个数都大于5，所以不会发生任何转换，也就没有意义
      .setInputCol("features").setOutputCol("indexedFeatures")
      .fit(df)


    //对原数据集划分训练数据（70%）和测试数据（30%）
    val Array(trainingData, testData): Array[Dataset[Row]] = df.randomSplit(Array(0.7, 0.3))

    /**
      * LR建模
      * setMaxIter设置最大迭代次数(默认100),具体迭代次数可能在不足最大迭代次数停止
      * setTol设置容错(默认1e-6),每次迭代会计算一个误差,误差值随着迭代次数增加而减小,当误差小于设置容错,则停止迭代
      * setRegParam设置正则化项系数(默认0),正则化主要用于防止过拟合现象,如果数据集较小,特征维数又多,易出现过拟合,考虑增大正则化系数
      * setElasticNetParam正则化范式比(默认0),正则化有两种方式:L1(Lasso)和L2(Ridge),L1用于特征的稀疏化,L2用于防止过拟合
      * setLabelCol设置标签列
      * setFeaturesCol设置特征列
      * setPredictionCol设置预测列
      * setThreshold设置二分类阈值
      */
    //设置逻辑回归参数
    val lr = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
      .setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)



    //转换器，将预测的类别重新转成字符型
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predectionLabel")
      .setLabels(labelIndexer.labels)


    //建立工作流
    val lrPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))

    //生成模型
    val model = lrPipeline.fit(trainingData)

    //预测
    val result = model.transform(testData)

    //打印结果
    result.show(200, false)

    //模型评估，预测准确性和错误率
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    val lrAccuracy: Double = evaluator.evaluate(result)

    println("Test Error = " + (1.0 - lrAccuracy))

    spark.stop()
  }
}


case class Iris(features: Vector, label: String)
