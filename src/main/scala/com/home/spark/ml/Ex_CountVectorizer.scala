package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

/**
  * @Author: xu.dm
  * @Date: 2020/1/8 9:10
  * @Version: 1.0
  * @Description:
  * CountVectorizer和CountVectorizerModel旨在帮助将文本文档的集合转换为令牌计数的向量。
  * 当先验字典不可用时，CountVectorizer可用作估计器以提取词汇表并生成CountVectorizerModel。
  * 该模型为词汇表上的文档生成稀疏向量表示方式，然后可以将其传递给其他算法，例如LDA。
  *
  * 在拟合过程中，CountVectorizer将选择整个语料库中按词频排列的前vocabSize词。
  * 可选参数minDF还通过指定一个术语必须出现在词汇表中的最小数量（或小于1.0的分数）来影响拟合过程。
  * 另一个可选的二进制切换参数控制输出向量。如果将其设置为true，则所有非零计数都将设置为1。
  * 这对于模拟二进制而不是整数计数的离散概率模型特别有用。
  **/
object Ex_CountVectorizer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark ml label")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")


    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      //词汇表最大容量，维度大小，本例是出现最高的三个单词
      .setVocabSize(3)
      //至少在两个文档中出现过的词
      .setMinDF(2)
      .fit(df)

    //查看词汇表里的单词
    println("vocabulary词库："+cvModel.vocabulary.mkString(",")) //b,a,c

    cvModel.transform(df).show(false)
    // todo  features表示向量，表示模型词库在待训练文本中出现向量标识
    //       按照词库(b,a,c)的顺序，在df中出现的词向量，即(维度大小,[序号],[频率])

//    vocabulary词库：b,a,c
//    +---+---------------+-------------------------+
//    |id |words          |features                 |
//    +---+---------------+-------------------------+
//    |0  |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
//    |1  |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
//    +---+---------------+-------------------------+

    val model = new CountVectorizerModel(Array("d","a","f","c"))
        .setInputCol("words").setOutputCol("features")
    model.transform(df).show(false)

//    +---+---------------+-------------------+
//    |id |words          |features           |
//    +---+---------------+-------------------+
//    |0  |[a, b, c]      |(4,[1,3],[1.0,1.0])|
//    |1  |[a, b, b, c, a]|(4,[1,3],[2.0,1.0])|
//    +---+---------------+-------------------+

    spark.stop()

  }
}
