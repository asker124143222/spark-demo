package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector

/**
  * @Author: xu.dm
  * @Date: 2020/1/2 17:01
  * @Version: 1.0
  * @Description: 机器学习，训练样本数据，给生产数据打标签
  *               样本训练数据中带有hello的文本，打标签为1，否则为0
  *               通过训练模型，我们希望待测试数据同样用这种方式打上标签。
  **/
object Ex_pipeline {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark ml label")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val error_count = spark.sparkContext.longAccumulator("error_count")

    //载入训练数据，数据手工训练，给带有hello的数据打上1.0的标签，给没有hello的数据打上0.0
    val lineRDD: RDD[String] = spark.sparkContext.textFile("input/sample.txt")

    //rdd转换成df或者ds需要SparkSession实例的隐式转换
    //导入隐式转换，注意这里的spark不是包名，而是SparkSession的对象名
    import spark.implicits._


    //生成训练数据，标签数据必须为double
    val training: DataFrame = lineRDD.map(line => {
      val strings: Array[String] = line.split(",")
      if (strings.length == 3) {
        (strings(0), strings(1), strings(2).toDouble)
      }
      else {
        error_count.add(1)
        ("-1", strings.mkString(" "), 0.0)
      }

    })
//      .filter(s => !s._1.equals("-1"))
      .filter(!_._1.equals("-1"))
      .toDF("id", "text", "label")

    training.printSchema()
    training.show()

    println(s"错误数据计数 : ${error_count.value}")


    //Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    //Transformer，转换器，字符解析，转换输入文本，以空格分隔，转成小写词
    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")


    //Transformer,转换器，哈希转换，以哈希方式将词转换成词频，转成特征向量
    val hashTF: HashingTF = new HashingTF()
      .setNumFeatures(1000) //缺省是2^18
      .setInputCol(tokenizer.getOutputCol).setOutputCol("features")

    //打印hashingTF生成的稀疏向量长什么样
    val wordsData = tokenizer.transform(training)
    wordsData.collect().foreach(println)

    println()

    val hashData = hashTF.transform(wordsData)
    hashData.collect().foreach(println)

    //[0,why hello world JAVA,1.0,WrappedArray(why, hello, world, java),(1000,[48,150,967,973],[1.0,1.0,1.0,1.0])]
    //向量长度1000，和setNumFeatures的值一致，然后索引是[48,150,967,973],值都是1.0，实际就是命中hash桶


    //Estimator，预测器或估算器，逻辑回归，10次最大迭代
    val lr: LogisticRegression = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
//    val lr: LogisticRegression = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
//  .setLabelCol("label").setFeaturesCol("features")

    //预测器通过 fit() 方法，接收一个 DataFrame 并产出一个模型（fit代表拟合）
    //封装流水线,包含两个转换器（实际包含两个模型），一个估算器（包含一个算法）
    //因为还有评估器，所以需要训练生成最终模型
    val pipeline: Pipeline = new Pipeline().setStages(Array(tokenizer, hashTF, lr))


    // Fit the pipeline to training documents.
    //训练，生成最终模型
    val model: PipelineModel = pipeline.fit(training)

    // 可以选择保存模型到磁盘
    model.write.overwrite().save("/tmp/spark-logistic-regression-model")
    // 重新加载回来
    //    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")


    // 保存未训练（unfit）的流水线到底盘
    //    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    //重新加载流水线
    //    val samePipeline = Pipeline.load("/tmp/unfit-lr-model")


    //加载待分析数据
    val testRDD: RDD[String] = spark.sparkContext.textFile("input/w1.txt")
    val test: DataFrame = testRDD.map(line => {
      val strings: Array[String] = line.split(",")
      if (strings.length == 2) {
        (strings(0), strings(1))
      }
      else {
        //        error_count.add(1)
        ("-1", strings.mkString(" "))
      }

    }).filter(s => !s._1.equals("-1"))
      .toDF("id", "text")

    println()

    //对给定数据进行预测
    val result = model.transform(test)
    result.show(false)
    result
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach {
        case Row(id: String, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

    spark.stop()

  }
}

/* 运行结果
(0, hello world) --> prob=[0.02467400198786794,0.975325998012132], prediction=1.0
(1, hello java test num) --> prob=[0.48019580016300345,0.5198041998369967], prediction=1.0
(2, test hello scala) --> prob=[0.6270035488150222,0.3729964511849778], prediction=0.0
(3, j hello spark) --> prob=[0.031182836719302286,0.9688171632806978], prediction=1.0
(4, abc hello c#) --> prob=[0.006011466954209337,0.9939885330457907], prediction=1.0
(5, hell java spark) --> prob=[0.9210765571223096,0.07892344287769032], prediction=0.0
(6, hello java spark) --> prob=[0.1785326777978406,0.8214673222021593], prediction=1.0
(7, num he he java spark) --> prob=[0.6923088930430097,0.30769110695699026], prediction=0.0
(8, hello2 java spark) --> prob=[0.9016001424620457,0.09839985753795444], prediction=0.0
(9, hello do some thing java spark) --> prob=[0.1785326777978406,0.8214673222021593], prediction=1.0
(10, world hello java spark) --> prob=[0.05144953292014106,0.9485504670798589], prediction=1.0
*/
//probability 是预测概率向量，第一个值是不符合度，第二个值是符合度，
//prediction的标签取决于模型的阀值设置严格度