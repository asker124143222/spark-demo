package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2020/1/7 16:37
  * @Version: 1.0
  * @Description:
  * word2vector 是google开源的一个生成词向量的工具，
  * 以语言模型为优化目标，迭代更新训练文本中的词向量，最终收敛获得词向量。
  * 词向量可以作为文本分析中重要的特征，在分类问题、标注问题等场景都有着重要的应用价值。
  * 由于是用向量表示，而且用较好的训练算法得到的词向量的向量一般是有空间上的意义的，
  * 也就是说，将所有这些向量放在一起形成一个词向量空间，
  * 而每一向量则为该空间中的一个点，在这个空间上的词向量之间的距离度量也可以表示对应的两个词之间的“距离”。
  * 所谓两个词之间的“距离”，就是这两个词之间的语法，语义之间的相似性。
  *
  * 一个比较实用的场景是找同义词，得到词向量后，假如对于词来说，想找出与这个词相似的词，
  * 这个场景对人来说都不轻松，毕竟比较主观，但是对于建立好词向量后的情况，
  * 对计算机来说，只要拿这个词的词向量跟其他词的词向量一一计算欧式距离或者cos距离，得到距离小于某个值那些词，就是它的同义词。
  *
  * 这个特性使词向量很有意义，自然会吸引很多人去研究，google的word2vec模型也是基于这个做出来的。
  *
  * Word2Vec 是一种著名的 词嵌入（Word Embedding） 方法，
  * 它可以计算每个单词在其给定语料库环境下的 分布式词向量（Distributed Representation，亦直接被称为词向量）。
  * 词向量表示可以在一定程度上刻画每个单词的语义。
  *
  * 如果词的语义相近，它们的词向量在向量空间中也相互接近，这使得词语的向量化建模更加精确，可以改善现有方法并提高鲁棒性。
  * 词向量已被证明在许多自然语言处理问题，如：机器翻译，标注问题，实体识别等问题中具有非常重要的作用。
  *
  * ​ Word2vec是一个Estimator，它采用一系列代表文档的词语来训练word2vecmodel。
  * 该模型将每个词语映射到一个固定大小的向量。
  * word2vecmodel使用文档中每个词语的平均数来将文档转换为向量，然后这个向量可以作为预测的特征，来计算文档相似度计算等等。
  **/
object Ex_Word2Vec {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark ml label")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val list = Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    )
    list.foreach(a=>println(a.mkString(",")))
    println()

    val tuples: Seq[Tuple1[Array[String]]] = list.map(Tuple1.apply)
    tuples.foreach(a=>println(a._1.mkString(",")))
    println()

    val  myText = spark.createDataFrame(tuples).toDF("myText")
    myText.show(false)
    println()

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    documentDF.show(false)

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3) //向量大小
      .setMinCount(0) //出现最小次数，大于0次
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.show(false)
    result.collect().foreach {
      case Row(text: Seq[_], features: Vector) =>
        println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }


     val searchResult =  model.findSynonyms("Spark",200)
    searchResult.show(false)

    spark.stop()
  }
}
