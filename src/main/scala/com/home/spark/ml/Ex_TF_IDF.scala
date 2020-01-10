package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2020/1/7 11:44
  * @Version: 1.0
  * @Description: TF-IDF (Term frequency-inverse document frequency)
  *               TF-IDF(HashingTF and IDF)
  *               “词频－逆向文件频率”（TF-IDF）是一种在文本挖掘中广泛使用的特征向量化方法，它可以体现一个文档中词语在语料库中的重要程度。
  *               词语由t表示，文档由d表示，语料库由D表示。词频TF(t,d)是词语t在文档d中出现的次数。文件频率DF(t,D)是包含词语的文档的个数。
  *               如果我们只使用词频来衡量重要性，很容易过度强调在文档中经常出现，却没有太多实际信息的词语，比如“a”，“the”以及“of”。
  *               如果一个词语经常出现在语料库中，意味着它并不能很好的对文档进行区分。TF-IDF就是在数值化文档信息，衡量词语能提供多少信息以区分文档。
  *
  *               公式中使用log函数，当词出现在所有文档中时，它的IDF值变为0。加1是为了避免分母为0的情况。IDF 度量值表示如下：
  *               IDF(t,D)=log((|D|+1)/(DF(t,D)+1))
  *               ，此处的|D]语料库中总的文档数，
  *
  *               术语频率和文档频率的定义有几种变体。在MLlib中，我们将TF和IDF分开以使其灵活。TF-IDF 度量值表示如下：
  *               TFIDF(t,d,D)=TF(t,d)*IDF(t,D).
  *
  *               TF: HashingTF 是一个Transformer，在文本处理中，接收词条的集合然后把这些集合转化成固定长度的特征向量。
  *               这个算法在哈希的同时会统计各个词条的词频。
  *               IDF: IDF是一个Estimator，在一个数据集上应用它的fit（）方法，产生一个IDFModel。
  *               该IDFModel 接收特征向量（由HashingTF产生），然后计算每一个词在文档中出现的频次。
  *               IDF会减少那些在语料库中出现频率较高的词的权重。
  *               在下面的代码段中，我们以一组句子开始。首先使用分解器Tokenizer把句子划分为单个词语。
  *               对每一个句子（词袋），我们使用HashingTF将句子转换为特征向量，最后使用IDF重新调整特征向量。
  *               这种转换通常可以提高使用文本特征的性能。
  **/
object Ex_TF_IDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark ml label")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark,I love spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat,I like it")
    )).toDF("label", "sentence")

    //分词
    val tokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words")
      .setPattern("\\W").setToLowercase(true)
    val wordsData: DataFrame = tokenizer.transform(sentenceData)
    wordsData.show(false)


    //hash向量化，hash桶的数量少容易出现hash碰撞
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rowFeatures").setNumFeatures(2000)
    val featurizedData  = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.select("words","rowFeatures").show(false)

    //IDF是一个评估器，需要使用fit进行转换，生成模型
    val idf = new IDF().setInputCol("rowFeatures").setOutputCol("features")
    val model: IDFModel = idf.fit(featurizedData)

    //IDF减少那些在语料库中出现频率较高的词的权重
    //通过TF-IDF转换后得到带权重的向量值
    val rescaleData = model.transform(featurizedData)
    rescaleData.select("words","rowFeatures","features").show(false)

    spark.stop()
  }
}
